// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library server_test;

import "package:unittest/unittest.dart";
import "package:unittest/mock.dart";
import "package:clean_sync/server.dart";
import "dart:async";

class DataProviderMock extends Mock implements DataProvider {
  final futureMock = new FutureMock();

  DataProviderMock() {
    when(callsTo("data")).alwaysReturn(futureMock);
    when(callsTo("diffFromVersion")).alwaysReturn(futureMock);
    when(callsTo("add")).alwaysReturn(futureMock);
    when(callsTo("change")).alwaysReturn(futureMock);
    when(callsTo("remove")).alwaysReturn(futureMock);
  }
}
class FutureMock extends Mock implements Future {}

void main() {
  group("Publisher", () {

    Publisher publisher;
    Map request, args;
    DataProviderMock dataProvider;
    var generator, _generator;

    void verifyGeneratorCalledOnceWithArgs(args) {
      _generator.getLogs().verify(happenedOnce);
      expect(_generator.getLogs().first.args.first, equals(args));
    }

    setUp((){
      publisher = new Publisher();
      args = {"long": true};
      dataProvider = new DataProviderMock();
      _generator = new Mock()
          ..when(callsTo("handle")).alwaysReturn(dataProvider);
      generator = (args) => _generator.handle(args);
    });

    test("publish collection.", () {
      // when
      publisher.publish("months", null);

      // then
      expect(publisher.isPublished("months"), isTrue);
      expect(publisher.isPublished("people"), isFalse);
    });

    test("handle get data.", () {
      // given
      request = {"args": {
        "action": "get_data",
        "collection": "months",
        "args": args}
      };
      publisher.publish("months", generator);

      // when
      var result = publisher.handleSyncRequest(request);

      // then
      verifyGeneratorCalledOnceWithArgs(args);
      dataProvider.getLogs(callsTo('data')).verify(happenedOnce);
      expect(result, equals(dataProvider.futureMock));
    });

    test("handle get diff.", () {
      // given
      request = {"args": {
        "action": "get_diff",
        "collection": "months",
        "args": args}
      };

      // when
      var result = publisher.handleSyncRequest(request);

      //then
      verifyGeneratorCalledOnceWithArgs(args);
      dataProvider.getLogs(callsTo('diffFromVersion')).verify(happenedOnce);
      expect(result, equals(dataProvider.futureMock));
    });

    test("handle add.", () {
      // given
      request = {"args": {
        "action": "add",
        "collection": "months",
        "args": args}
      };

      // when
      var result = publisher.handleSyncRequest(request);

      //then
      verifyGeneratorCalledOnceWithArgs(args);
      dataProvider.getLogs(callsTo('add')).verify(happenedOnce);
      expect(result, equals(dataProvider.futureMock));
    });

    test("handle change.", () {
      // given
      request = {"args": {
        "action": "change",
        "collection": "months",
        "args": args}
      };

      // when
      var result = publisher.handleSyncRequest(request);

      //then
      verifyGeneratorCalledOnceWithArgs(args);
      dataProvider.getLogs(callsTo('change')).verify(happenedOnce);
      expect(result, equals(dataProvider.futureMock));
    });

    test("handle remove.", () {
      // given
      request = {"args": {
        "action": "remove",
        "collection": "months",
        "args": args}
      };

      // when
      var result = publisher.handleSyncRequest(request);

      //then
      verifyGeneratorCalledOnceWithArgs(args);
      dataProvider.getLogs(callsTo('remove')).verify(happenedOnce);
      expect(result, equals(dataProvider.futureMock));
    });
  });
}