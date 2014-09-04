// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library map_cursor_test;

import 'package:unittest/unittest.dart';
import 'package:clean_sync/clean_cursors.dart';


void main() {

  group('(MapCursor)',() {

    setUp((){});

    test('Dummy. (T01)', () {
      Reference ref = new Reference.from({'hello': { 'persistent': 'data'}});
      MapCursor hello = new MapCursor(ref, ['hello']);

      expect(hello.value, equals(ref.value.lookup('hello')));
    });

    test('Dummy. (T02)', () {
      Reference ref = new Reference.from({'hello': { 'persistent': 'data'}});
      MapCursor hello = new MapCursor(ref, ['hello']);
      String data = hello['persistent'];

      expect(data, equals('data'));
    });

    test('Dummy. (T03)', () {
      Reference ref = new Reference.from({'hello': { 'persistent': 'data'}});
      MapCursor map = new MapCursor(ref, []);
      String data = map['hello']['persistent'];

      expect(data, equals('data'));
    });

    test('Change. (T04)', () {
      Reference ref = new Reference.from({'hello': { 'persistent': 'data'}});
      MapCursor map = new MapCursor(ref, []);
      map['hello']['persistent'] = 'newData';

      expect(map['hello']['persistent'], equals('newData'));
    });

    test('Add. (T05)', () {
      Reference ref = new Reference.from({'hello': { 'persistent': 'data'}});
      MapCursor map = new MapCursor(ref, []);
      map['hello']['listenable'] = 'data';

      expect(map['hello']['listenable'], equals('data'));
    });

    test('Remove. (T05)', () {
      Reference ref = new Reference.from({'hello': { 'persistent': 'data'}});
      MapCursor map = new MapCursor(ref, []);
      map['hello'].remove('persistent');
      expect(() => map['hello']['persistent'], throws);
    });

    group('changes', () {
      test('listening', () {
        Reference ref = new Reference.from({'hello': { 'persistent': 'data'}, 'bye': 'clean_data'});
        MapCursor map = new MapCursor(ref, []);
        map.onChange.listen(expectAsync((_) => null));
        map['hello'].onChange.listen(expectAsync((_) => null));
        map.ref('bye').onChange.listen(expectAsync((_) => null, count: 0));
        map['hello'].ref('persistent').onChange.listen(expectAsync((_) => null));
        map['hello']['persistent'] = 'listenable data';
      });

      test('listening sync', () {
        Reference ref = new Reference.from({'hello': { 'persistent': 'data'}, 'bye': 'clean_data'});
        MapCursor map = new MapCursor(ref, []);
        map.onChangeSync.listen(expectAsync((_) => null));
        map['hello'].onChangeSync.listen(expectAsync((_) => null));
        map.ref('bye').onChangeSync.listen(expectAsync((_) => null, count: 0));
        map['hello'].ref('persistent').onChangeSync.listen(expectAsync((_) => null));
        map['hello']['persistent'] = 'listenable data';
      });

      test('listening sync, deep change', () {
        Reference ref = new Reference.from({'hello': { 'persistent': 'data'}, 'bye': 'clean_data'});
        MapCursor map = new MapCursor(ref, []);
        map.onChangeSync.listen(expectAsync((_) => null));
        map['hello'].onChange.listen(expectAsync((_) => null));
        map.ref('bye').onChange.listen(expectAsync((_) => null, count: 0));
        map['hello'].ref('persistent').onChange.listen(expectAsync((_) => null));
        map.value = {'hello': { 'persistent': 'listenable data'}, 'bye': 'clean_data'};
      });

      test('listening sync, deep change', () {
        Reference ref = new Reference.from({'hello': { 'persistent': 'data'}, 'bye': 'clean_data'});
        MapCursor map = new MapCursor(ref, []);
        map.onChangeSync.listen(expectAsync((_) => null));
        map['hello'].onChangeSync.listen(expectAsync((_) => null));
        map.ref('bye').onChangeSync.listen(expectAsync((_) => null, count: 0));
        map['hello'].ref('persistent').onChangeSync.listen(expectAsync((_) => null));
        map.value = {'hello': { 'persistent': 'listenable data'}, 'bye': 'clean_data'};
      });
    });
  });
}