library reference_test;

import 'package:clean_sync/clean_cursors.dart';
import 'package:unittest/unittest.dart';
import 'package:persistent/persistent.dart';

main() {
  group('listeners', () {
    test('', () {
      Reference ref = new Reference.from({
      'a': { 'b': { 'c': 'd'}},
      'e': { 'f': 'g'}
      });
      MapCursor map = ref.cursor;

      MapCursor a = map['a'];
      a.onChange.listen((_) => null);
      a.onChangeSync.listen((_) => null);
      MapCursor b = map['a']['b'];
      b.onChange.listen((_) => null);
      b.onChangeSync.listen((_) => null);
      Cursor f = map['e'].ref('f');
      f.onChange.listen((_) => null);
      f.onChangeSync.listen((_) => null);
      a.dispose();
      f.dispose();
      b.dispose();

      //does not throw
    });
  });
}