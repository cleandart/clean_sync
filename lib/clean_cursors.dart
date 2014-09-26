library clean_cursors;

import 'package:persistent/persistent.dart';
import 'package:persistent/persistent.dart' as per;
import 'dart:async';
import 'dart:collection';
import 'package:clean_data/clean_data.dart';

part 'src_cursors/cursor.dart';
part 'src_cursors/map_cursor.dart';
part 'src_cursors/reference.dart';
part 'src_cursors/list_cursor.dart';
part 'src_cursors/set_cursor.dart';

/*
DataReference refFromCursor(Cursor cursor) {
  DataReference ref = new DataReference(cursor.value);
  cursor.onChange.listen((_) => ref.value = cursor.value);
  return ref;
}
*/