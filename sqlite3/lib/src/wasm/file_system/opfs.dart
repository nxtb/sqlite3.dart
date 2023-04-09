import 'dart:html';
import 'dart:typed_data';

import 'package:meta/meta.dart';
import 'package:path/path.dart' as p;

import '../../constants.dart';
import '../file_system.dart';
import '../js_interop.dart';

@internal
enum FileType {
  database('/database'),
  journal('/database-journal');

  final String filePath;

  const FileType(this.filePath);

  static final byName = {
    for (final entry in values) entry.filePath: entry,
  };

  static final _validNames = values.map((e) => e.filePath).join(', ');
}

/// A [FileSystem] for the `sqlite3` wasm library based on the [file system access API].
///
/// By design, this file system can only store two files: `/database` and
/// `/database-journal`. Thus, when this file system is used, the only sqlite3
/// database that will be persisted properly is the one at `/database`.
///
/// The limitation of only being able to store two files comes from the fact
/// that we can't synchronously _open_ files in with the file system access API,
/// only reads and writes are synchronous.
/// By having a known amount of files to store, we can simply open both files
/// in [OpfsFileSystem.inDirectory] or [OpfsFileSystem.loadFromStorage], which
/// is asynchronous too. The actual file system work, which needs to be
/// synchronous for sqlite3 to function, does not need any further wrapper.
///
/// Please note that [OpfsFileSystem]s are only available in dedicated web workers,
/// not in the JavaScript context for a tab or a shared web worker.
///
/// [file system access API]: https://developer.mozilla.org/en-US/docs/Web/API/File_System_Access_API
class OpfsFileSystem implements FileSystem {
  // The storage idea here is to open sync file handles at the beginning, so
  // that no new async open needs to happen when these callbacks are invoked by
  // sqlite3.
  // We open a sync file for each stored file ([FileType]), plus a meta file
  // file handle that describes whether a file exists and what length it has.
  // This meta handle is necessary because, despite the standard saying that the
  // methods should be synchronous, `truncate`and `getSize` are asynchronous
  // JavaScript methods in Chrome and Safari.
  // Handles for stored files just store the raw data directly. The layout of
  // meta file is described in [_MetaInformation].
  final _MetaInformation _metaInformation;
  final Map<FileType, FileSystemSyncAccessHandle> _files;

  final FileSystem _memory = FileSystem.inMemory();

  // Whether close, flush, getSize and truncate are synchronous in the browser.
  // An earlier version of the web specification described these as asynchronous
  // and Safari still returns promises. Other browsers made them synchronous.
  final bool underlyingApiIsSynchronous;

  OpfsFileSystem._(FileSystemSyncAccessHandle meta, this._files,
      this.underlyingApiIsSynchronous)
      : _metaInformation = _MetaInformation(meta);

  /// Loads an [OpfsFileSystem] in the desired [path] under the root directory
  /// for OPFS as given by `navigator.storage.getDirectory()` in JavaScript.
  ///
  /// Throws a [FileSystemException] if OPFS is not available - please note that
  /// this file system implementation requires a recent browser and only works
  /// in dedicated web workers.
  static Future<OpfsFileSystem> loadFromStorage(String path) async {
    final storage = storageManager;
    if (storage == null) {
      throw FileSystemException(
          SqlError.SQLITE_ERROR, 'storageManager not supported by browser');
    }

    var opfsDirectory = await storage.directory;

    for (final segment in p.split(path)) {
      opfsDirectory = await opfsDirectory.getDirectory(segment, create: true);
    }

    return inDirectory(opfsDirectory);
  }

  /// Loads an [OpfsFileSystem] in the desired [root] directory, which must be
  /// a Dart wrapper around a [FileSystemDirectoryHandle].
  ///
  /// [FileSystemDirectoryHandle]: https://developer.mozilla.org/en-US/docs/Web/API/FileSystemDirectoryHandle
  static Future<OpfsFileSystem> inDirectory(Object root) async {
    Future<FileSystemSyncAccessHandle> open(String name) async {
      final handle = await (root as FileSystemDirectoryHandle)
          .openFile(name, create: true);
      return await handle.createSyncAccessHandle();
    }

    final meta = await open('meta');
    await meta.truncate(_MetaInformation.totalSize);
    final files = {
      for (final type in FileType.values) type: await open(type.name)
    };

    final getSizeOperation = meta.getSize();
    bool isSynchronous;
    if (getSizeOperation is int) {
      isSynchronous = true;
    } else {
      await promiseToFuture<Object?>(getSizeOperation);
      isSynchronous = false;
    }

    return OpfsFileSystem._(meta, files, isSynchronous);
  }

  void _markExists(FileType type, bool exists) {
    _metaInformation
      ..setFileExists(type, exists)
      ..write();
  }

  FileType? _recognizeType(String path) {
    return FileType.byName[path];
  }

  @override
  void clear() {
    _memory.clear();

    for (final entry in _files.keys) {
      _metaInformation.setFileExists(entry, false);
    }
    _metaInformation.write();
  }

  @override
  void createFile(String path,
      {bool errorIfNotExists = false, bool errorIfAlreadyExists = false}) {
    final type = _recognizeType(path);
    if (type == null) {
      throw ArgumentError.value(
        path,
        'path',
        'Invalid path for OPFS file system, only ${FileType._validNames} are '
            'supported!',
      );
    } else {
      _metaInformation.read();
      final exists = _metaInformation.fileExists(type);

      if ((exists && errorIfAlreadyExists) || (!exists && errorIfNotExists)) {
        throw FileSystemException();
      }

      if (!exists) {
        _metaInformation
          ..setFileExists(type, true)
          ..setFileSize(type, 0)
          ..write();

        if (underlyingApiIsSynchronous) {
          // If we have a synchronous FS api, we can use truncate directly.
          // Otherwise we'd have to await it which we cannot do here.
          _files[type]!.truncate(0);
        }
      }
    }
  }

  @override
  String createTemporaryFile() {
    return _memory.createTemporaryFile();
  }

  @override
  void deleteFile(String path) {
    final type = _recognizeType(path);
    if (type == null) {
      return _memory.deleteFile(path);
    } else {
      _markExists(type, false);
    }
  }

  @override
  bool exists(String path) {
    final type = _recognizeType(path);
    if (type == null) {
      return _memory.exists(path);
    } else {
      _metaInformation.read();
      return _metaInformation.fileExists(type);
    }
  }

  @override
  List<String> get files {
    _metaInformation.read();

    return [
      for (final type in FileType.values)
        if (_metaInformation.fileExists(type)) type.filePath,
      ..._memory.files,
    ];
  }

  @override
  int read(String path, Uint8List target, int offset) {
    final type = _recognizeType(path);
    if (type == null) {
      return _memory.read(path, target, offset);
    } else {
      final handle = _files[type]!;
      Uint8List adaptedTarget;

      if (underlyingApiIsSynchronous) {
        adaptedTarget = target;
      } else {
        // Since truncate is asynchronous, it may be that the file as seen by
        // the browser is larger than we want it to be.
        final length = _metaInformation.fileSize(type);
        final bytesAvailable = length - offset;
        if (bytesAvailable > target.length) {
          adaptedTarget =
              target.buffer.asUint8List(target.offsetInBytes, bytesAvailable);
        } else {
          adaptedTarget = target;
        }
      }

      return handle.read(adaptedTarget, FileSystemReadWriteOptions(at: offset));
    }
  }

  @override
  int sizeOfFile(String path) {
    final type = _recognizeType(path);
    if (type == null) {
      return _memory.sizeOfFile(path);
    } else {
      // getSize() is asynchronous in some browsers, but we need a synchronous
      // API. We cache this information in _metaInformation to support this
      // otherwise.
      if (underlyingApiIsSynchronous) {
        return _files[type]!.getSizeAsInt();
      } else {
        return _metaInformation.fileSize(type);
      }
    }
  }

  @override
  void truncateFile(String path, int length) {
    final type = _recognizeType(path);

    if (type == null) {
      _memory.truncateFile(path, length);
    } else {
      if (underlyingApiIsSynchronous) {
        _files[type]!.truncateSync(length);
      } else {
        final oldSize = _metaInformation.fileSize(type);
        if (oldSize < length) {
          // The truncate operation adds a bunch of zeroes at the end
          final zeroes = Uint8List(length - oldSize);
          _files[type]!.write(zeroes, FileSystemReadWriteOptions(at: oldSize));
        }
      }

      _metaInformation
        ..setFileSize(type, length)
        ..write();
    }
  }

  @override
  void write(String path, Uint8List bytes, int offset) {
    final type = _recognizeType(path);
    if (type == null) {
      _memory.write(path, bytes, offset);
    } else {
      _files[type]!.write(bytes, FileSystemReadWriteOptions(at: offset));

      if (!underlyingApiIsSynchronous) {
        // This write could have changed the actual file size, which needs to
        // be adopted now.
        final oldSize = _metaInformation.fileSize(type);
        final end = offset + bytes.length;

        if (end > oldSize) {
          _metaInformation
            ..setFileSize(type, end)
            ..write();
        }
      }
    }
  }

  Future<void> close() async {
    _metaInformation._metaHandle.close();

    for (final entry in _files.values) {
      entry.close();
    }
  }
}

class _MetaInformation {
  // 8 bytes for each file. First byte is a 0/1 describing whether the file
  // exists. Then there are three unused bytes, followed by an int32 (in big
  // endian order) describing the length of the file.
  static const _entrySize = 8;
  static final totalSize = _entrySize * FileType.values.length;

  final Uint8List _data;
  final ByteData _byteData;

  final FileSystemSyncAccessHandle _metaHandle;

  _MetaInformation._(this._metaHandle, this._data, this._byteData);

  factory _MetaInformation(FileSystemSyncAccessHandle meta) {
    final bytes = Uint8List(_entrySize * FileType.values.length);
    return _MetaInformation._(meta, bytes, bytes.buffer.asByteData());
  }

  void read() {
    _metaHandle.read(_data, FileSystemReadWriteOptions(at: 0));
  }

  void write() {
    _metaHandle.write(_data, FileSystemReadWriteOptions(at: 0));
  }

  bool fileExists(FileType type) {
    return _data[_entrySize * type.index] != 0;
  }

  void setFileExists(FileType type, bool exists) {
    _data[_entrySize * type.index] = exists ? 1 : 0;
    if (!exists) {
      setFileSize(type, 0);
    }
  }

  int fileSize(FileType type) {
    return _byteData.getInt32(_entrySize * type.index + 4);
  }

  void setFileSize(FileType type, int size) {
    return _byteData.setInt32(_entrySize * type.index + 4, size);
  }
}
