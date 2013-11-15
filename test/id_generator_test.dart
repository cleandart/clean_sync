// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library id_generator_test;

import 'package:unittest/unittest.dart';
import 'package:clean_sync/client.dart';

void main() {
  group('IdGenerator', () {

    test('correctly returns id when prefix is empty string.',() {
      // given
      var generator = new IdGenerator('');

      // then
      expect(generator.next(), equals('-1'));
      expect(generator.next(), equals('-2'));
      expect(generator.next(), equals('-3'));
    });

    test('correctly returns bigger ids.',() {
      // given
      var generator = new IdGenerator('');

      // when
      for(int i=1; i<1000; i++) {
        generator.next();
      }

      // then
      expect(generator.next(),equals('-rs'));
    });

    test('correctly returns prefix.',() {
      // given
      var generator = new IdGenerator('prefix');

      // then
      expect(generator.next(), equals('prefix-1'));
    });

    test('correctly appends numbers to prefix.',() {
      // given
      var generator = new IdGenerator('prefix2');

      // when
      for(int i=1; i<211994; i++) {
        generator.next();
      }

      // then
      expect(generator.next(), equals('prefix2-4jkq'));
    });
  });
}
