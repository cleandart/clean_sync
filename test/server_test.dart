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
  group("Publisher", () {

    Publisher publisher;
    ClientRequest request;
    Map args;
    var _id, data, author;
    DataProviderMock dataProvider;
    var _generator;

    DataProvider generator(args) => _generator.handle(args);

    void verifyGeneratorCalledOnceWithArgs(args) {
      _generator.getLogs().verify(happenedOnce);
      expect(_generator.getLogs().first.args.first, equals(args));
    }

    setUp((){
      publisher = new Publisher();
      args = {"long": true};
      _id = 7;
      data = {"data": "some data"};
      author = "someone";
      dataProvider = new DataProviderMock();
      _generator = new Mock()
          ..when(callsTo("handle")).alwaysReturn(dataProvider);
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
      request = new ClientRequest(null, {
        "action": "get_data",
        "collection": "months",
        "args": args
      });
      publisher.publish("months", generator);

      // when
      var result = publisher.handleSyncRequest(request);

      // then
      verifyGeneratorCalledOnceWithArgs(args);
      dataProvider.getLogs(callsTo('data')).verify(happenedOnce);
      expect(result, equals(dataProvider.responseFuture));
    });

    test("handle get diff.", () {
      // given
      request = new ClientRequest(null, {
        "action": "get_diff",
        "collection": "months",
        "version": 5,
        "args": args
      });

      // when
      var result = publisher.handleSyncRequest(request);

      //then
      verifyGeneratorCalledOnceWithArgs(args);
      var callToDiff = dataProvider.getLogs(callsTo('diffFromVersion'));
      callToDiff.verify(happenedOnce);
      expect(callToDiff.first.args.first, equals(5));
      expect(result, equals(dataProvider.responseFuture));
    });

    test("handle add.", () {
      // given
      request = new ClientRequest(null, {
        "action": "add",
        "collection": "months",
        "_id": _id,
        "data": data,
        "author": author,
        "args": args
      });

      // when
      var result = publisher.handleSyncRequest(request);

      //then
      verifyGeneratorCalledOnceWithArgs(args);
      var callToDiff = dataProvider.getLogs(callsTo('add'));
      callToDiff.verify(happenedOnce);
      expect(callToDiff.first.args[0], equals(_id));
      expect(callToDiff.first.args[1], equals(data));
      expect(callToDiff.first.args[2], equals(author));
      expect(result, equals(dataProvider.responseFuture));
    });

    test("handle change.", () {
      // given
      request = new ClientRequest(null, {
        "action": "change",
        "collection": "months",
        "_id": _id,
        "data": data,
        "author": author,
        "args": args
      });

      // when
      var result = publisher.handleSyncRequest(request);

      //then
      verifyGeneratorCalledOnceWithArgs(args);
      var callToDiff = dataProvider.getLogs(callsTo('change'));
      callToDiff.verify(happenedOnce);
      expect(callToDiff.first.args[0], equals(_id));
      expect(callToDiff.first.args[1], equals(data));
      expect(callToDiff.first.args[2], equals(author));
      expect(result, equals(dataProvider.responseFuture));
    });

    test("handle remove.", () {
      // given
      request = new ClientRequest(null, {
        "action": "remove",
        "collection": "months",
        "_id": _id,
        "author": author,
        "args": args
      });

      // when
      var result = publisher.handleSyncRequest(request);

      //then
      verifyGeneratorCalledOnceWithArgs(args);
      var callToDiff = dataProvider.getLogs(callsTo('remove'));
      callToDiff.verify(happenedOnce);
      expect(callToDiff.first.args[0], equals(_id));
      expect(callToDiff.first.args[1], equals(author));
      expect(result, equals(dataProvider.responseFuture));
    });

    test("handle get server prefix.", () {
      request = new ClientRequest(null, {
        "action": "get_id_prefix",
      });

      // when
      Future<Map> result = publisher.handleSyncRequest(request);

      //then
      expect(result, completion(isMap));
      expect(result, completion(contains('id_prefix')));
    });

    test("get server prefix returns different values.", () {
      String prefix1 = publisher.getIdPrefix();
      String prefix2 = publisher.getIdPrefix();

      //then
      expect(prefix1 == prefix2, isFalse);
    });

  });
}
