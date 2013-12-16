// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library server_test;

import "package:unittest/unittest.dart";
import "package:unittest/mock.dart";
import "package:clean_sync/server.dart";
import "dart:async";
import 'package:clean_ajax/server.dart';

class DataProviderMock extends Mock implements DataProvider {
  final responseFuture = new FutureMock();

  DataProviderMock() {
    when(callsTo("data")).alwaysReturn(responseFuture);
    when(callsTo("diffFromVersion", anything)).alwaysReturn(responseFuture);
    when(callsTo("add")).alwaysReturn(responseFuture);
    when(callsTo("change")).alwaysReturn(responseFuture);
    when(callsTo("remove")).alwaysReturn(responseFuture);
  }
}
class FutureMock extends Mock implements Future {}

void main() {
  group('MongoProvider', () {
    MongoProvider months;
    Future ready;

    setUp(() {
      MongoDatabase mongodb = new MongoDatabase('mongodb://0.0.0.0/devel');
      ready = Future.wait(mongodb.init)
                    .then((_) => months = mongodb.collection('months'));
    });

    test('get data.', () {
      // when
      return ready.then((_) {
        // then
        return months.data().then((Map data) {
          expect(data['data'], equals([]));
        });
      });
    });

  });
}