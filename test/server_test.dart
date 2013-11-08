// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library server_test;

import "package:unittest/unittest.dart";
import "package:unittest/mock.dart";
import "package:clean_sync/server.dart";
import "dart:async";

class DataProviderMock extends Mock implements DataProvider {}
class FutureMock extends Mock implements Future {}

void main() {
  group("Publisher", () {

    test("publish collection.", () {
      // given
      var publisher = new Publisher();

      // when
      publisher.publish("months", null);

      // then
      expect(publisher.isPublished("months"), isTrue);
      expect(publisher.isPublished("people"), isFalse);
    });

    test("handle get data.", () {
      // given
      var publisher = new Publisher();
      var args = {"long": true};
      var request = {"args": {
        "action": "get_data",
        "collection": "months",
        "args": args}
      };
      var data = new FutureMock();
      var dataProvider = new DataProviderMock()
          ..when(callsTo('data')).alwaysReturn(data);
      var dataGenerator = new Mock()
          ..when(callsTo('handle')).alwaysReturn(dataProvider);
      publisher.publish("months", (args) => dataGenerator.handle(args));

      // when
      var result = publisher.handleSyncRequest(request);

      // then
      dataGenerator.getLogs().verify(happenedOnce);
      expect(dataGenerator.getLogs().first.args.first, equals(args));

      dataProvider.getLogs(callsTo('data')).verify(happenedOnce);

      expect(result, equals(data));
    });

    test("handle get diff.", () {
      //
    });

  });
}