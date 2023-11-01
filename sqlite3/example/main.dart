import 'dart:io';

import 'package:sqlite3/sqlite3.dart';

void main() {
  final db = sqlite3.openInMemory();
  db.createFunction(
    functionName: 'dart_version',
    function: (args) => Platform.version,
  );
  print(db.select('SELECT dart_version()'));
  db.dispose();
}
