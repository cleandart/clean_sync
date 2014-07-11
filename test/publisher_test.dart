// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library publisher_test;


import "package:unittest/unittest.dart";
import "package:mock/mock.dart";
import "package:clean_sync/server.dart";
import "dart:async";
import 'package:clean_ajax/server.dart';

class DataProviderMock extends Mock implements DataProvider {
  static const response = 'response';
  final responseFuture = new Future.value(response);
  final responseToDiff = new Future.value({'diff': response});

  DataProviderMock() {
    when(callsTo("data")).alwaysReturn(responseFuture);
    when(callsTo("diffFromVersion", anything)).alwaysReturn(responseToDiff);
    when(callsTo("add")).alwaysReturn(responseFuture);
    when(callsTo("change")).alwaysReturn(responseFuture);
    when(callsTo("remove")).alwaysReturn(responseFuture);
  }
}

class ServerRequestMock extends Mock implements ServerRequest {
  ServerRequestMock(args) {
    when(callsTo('get args')).alwaysReturn(args);
  }
}

void main(){
  run();
}

void run() {
  group("Publisher", () {

    Publisher publisher;
    ServerRequestMock request;
    Map args;
    var _id, data, author;
    DataProviderMock dataProvider;
    var _generator;

    Future<DataProvider> generator(args) => _generator.handle(args);

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
          ..when(callsTo("handle")).alwaysReturn(new Future.value(dataProvider));
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
      request = new ServerRequestMock({
        "action": "get_data",
        "collection": "months",
        "args": args
      });

      // when
      publisher.publish("months", generator);
      return publisher.handleSyncRequest(request).then((result) {
        // then
        verifyGeneratorCalledOnceWithArgs(args);
        dataProvider.getLogs(callsTo('data')).verify(happenedOnce);
        expect(result, equals(DataProviderMock.response));
      });
    });

    test("handle get diff.", () {
      // given
      request = new ServerRequestMock({
        "action": "get_diff",
        "collection": "months",
        "version": 5,
        "args": args
      });

      // when
      publisher.publish("months", generator);
      return publisher.handleSyncRequest(request).then((result) {
        //then
        verifyGeneratorCalledOnceWithArgs(args);
        var callToDiff = dataProvider.getLogs(callsTo('diffFromVersion'));
        callToDiff.verify(happenedOnce);
        expect(callToDiff.first.args.first, equals(5));
        expect(result, equals({'diff': DataProviderMock.response}));
      });
    });

    test("handle add.", () {
      // given
      request = new ServerRequestMock({
        "action": "add",
        "collection": "months",
        "_id": _id,
        "data": data,
        "author": author,
        "args": args
      });

      // when
      publisher.publish("months", generator);
      return publisher.handleSyncRequest(request).then((result) {
        //then
        verifyGeneratorCalledOnceWithArgs(args);
        var callToDiff = dataProvider.getLogs(callsTo('add'));
        callToDiff.verify(happenedOnce);
        expect(callToDiff.first.args[0], equals(data));
        expect(callToDiff.first.args[1], equals(author));
        expect(result, equals(DataProviderMock.response));
      });
    });

    test("handle change.", () {
      // given
      request = new ServerRequestMock({
        "action": "change",
        "collection": "months",
        "_id": _id,
        "change": data,
        "author": author,
        "args": args
      });

      publisher.publish("months", generator);

      // when
      return publisher.handleSyncRequest(request).then((result) {
        //then
        verifyGeneratorCalledOnceWithArgs(args);
        var callToDiff = dataProvider.getLogs(callsTo('change'));
        callToDiff.verify(happenedOnce);
        expect(callToDiff.first.args[0], equals(_id));
        expect(callToDiff.first.args[1], equals(data));
        expect(callToDiff.first.args[2], equals(author));
        expect(result, equals(DataProviderMock.response));
      });
    });

    test("handle remove.", () {
      // given
      request = new ServerRequestMock({
        "action": "remove",
        "collection": "months",
        "_id": _id,
        "author": author,
        "args": args
      });

      // when
      publisher.publish("months", generator);
      return publisher.handleSyncRequest(request).then((result) {
        //then
        verifyGeneratorCalledOnceWithArgs(args);
        var callToDiff = dataProvider.getLogs(callsTo('remove'));
        callToDiff.verify(happenedOnce);
        expect(callToDiff.first.args[0], equals(_id));
        expect(callToDiff.first.args[1], equals(author));
        expect(result, equals(DataProviderMock.response));
      });
    });

    test("handle get server prefix.", () {
      request = new ServerRequestMock({
        "action": "get_id_prefix",
      });

      // when
      publisher.publish("months", generator);
      Future<Map> result = publisher.handleSyncRequest(request);

      //then
      expect(result, completion(isMap));
      expect(result, completion(contains('id_prefix')));
    });
  });
}
