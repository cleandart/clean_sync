// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library client_test;

import 'package:unittest/unittest.dart';
import 'package:unittest/mock.dart';
import 'package:clean_sync/client.dart';
import 'package:clean_ajax/client.dart';
import 'package:clean_data/clean_data.dart';
import 'dart:async';

class ConnectionMock extends Mock implements Connection {}
class IdGeneratorMock extends Mock implements IdGenerator {}

void main() {
  group("Subscriber", () {
    Subscriber subscriber;
    ConnectionMock connection;
    IdGeneratorMock subscriptionIdGenerator, dataIdGenerator;
    Mock subscriptionFactory;

    setUp(() {
      connection = new ConnectionMock();
      connection.when(callsTo('sendRequest', anything))
        .alwaysReturn(new Future.value({'id_prefix': 'prefix'}));
      subscriptionIdGenerator = new IdGeneratorMock();
      dataIdGenerator = new IdGeneratorMock();
      subscriptionFactory = new Mock();
    });

    test('id negotiation.', () {
      // given
      subscriber = new Subscriber.config(connection, subscriptionIdGenerator,
          dataIdGenerator, null);

      // when
      var future = subscriber.init();

      // then
      var sendRequestCalls = connection.getLogs(callsTo('sendRequest'));
      sendRequestCalls.verify(happenedOnce);
      ClientRequest request = sendRequestCalls.first.args.first();
      expect(request.args, equals({"action": "get_id_prefix"}));

      return future.then((_) {
        subscriptionIdGenerator
            .getLogs(callsTo('set prefix', 'prefix')).verify(happenedOnce);
        dataIdGenerator
            .getLogs(callsTo('set prefix', 'prefix')).verify(happenedOnce);
      });
    });

    test('subscribe when _id_prefix was not obtained.', () {
      // given
      subscriber = new Subscriber.config(connection, subscriptionIdGenerator,
          dataIdGenerator, subscriptionFactory);

      // then
      expect(()=> subscriber.subscribe("months"),
          throwsA(new isInstanceOf<MissingIdPrefixException>
            ("MissingIdPrefixException")));
    });

    test('subscribe when _id_prefix was obtained.', () {
      // given
      Map args = {'key1': 'val1'};
      subscriptionIdGenerator.when(callsTo('next')).alwaysReturn('prefix-1');

      subscriber = new Subscriber.config(connection, dataIdGenerator,
          subscriptionIdGenerator, subscriptionFactory);

      // when
      var future = subscriber.init().then((_) {
        subscriber.subscribe("months", args);
      });

      return future.then((_) {
        expect(subscriptionFactory.getLogs().first.args,
            equals(['months', connection, 'prefix-1', dataIdGenerator, args]));
      });
    });
  });


  group("Subscription", () {
    ConnectionMock connection;
    IdGeneratorMock idGenerator;
    Subscription months;
    Data january;
    Future futureData = new Future.value({});

    setUp(() {
      connection = new ConnectionMock();
      connection.when(callsTo('sendRequest')).alwaysReturn(futureData);
      idGenerator = new IdGeneratorMock();
    });

    test("assign id to data.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      months = new Subscription('months', connection, 'author', idGenerator);
      january = new Data.from({'name': 'January', 'order': 1});

      // when
      months.collection.add(january);

      // then
      expect(months.collection.first['_id'], equals('prefix-1'));
    });

//    test("request initial data and handle response.", () {
//      // given
//      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
//      connection = new ConnectionMock();
//      futureData = new Future.value({'data': [{'_id': '21',
//        'name': 'February', 'order': 2}]});
//      connection.when(callsTo('sendRequest'))
//        .alwaysReturn(futureData);
//
//      // when
//      months = new Subscription('months', connection, 'author', idGenerator);
//
//      // then
//      var request = connection.getLogs().first.args[0]();
//      expect(request.type, equals("sync"));
//      expect(request.args, equals({"action": "get_data",
//                                   "collection": "months"}));
//
//      expect(months.collection.length, equals(1));
//      expect(months.collection.first.containsValue("February"), isTrue);
//    });

    test("send add-request.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      months = new Subscription('months', connection, 'author', idGenerator);
      january = new Data.from({'name': 'January', 'order': 1});

      // when
      months.collection.add(january);

      // then
      var request = connection.getLogs().last.args[0]();
      expect(request.type, equals("sync"));
      expect(request.args, equals({"action": "add", "collection": "months",
                                  "data": january, "author": "author"}));
    });

    test("send change-request.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      months = new Subscription('months', connection, 'author', idGenerator);
      january = new Data.from({'name': 'January', 'order': 1});

      // when
      months.collection.add(january);
      january.addAll({'length': 31});

      // then
      var request = connection.getLogs().last.args[0]();
      expect(request.type, equals("sync"));
      expect(request.args, equals({"action": "change", "collection": "months",
                                   "_id": "prefix-1", "data": {'length': 31},
                                   "author": "author"}));
    });

    test("send remove-request.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      months = new Subscription('months', connection, 'author', idGenerator);
      january = new Data.from({'_id': '12', 'name': 'January', 'order': 1});

      // when
      months.collection.remove(january);

      // then
      var request = connection.getLogs().last.args[0]();
      expect(request.type, equals("sync"));
      expect(request.args, equals({"action": "remove", "collection": "months",
                                   "_id": "12", "author": "author"}));
    });


  });
}
