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
class CommunicatorMock extends Mock implements Communicator {}

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
    Data january, february;
    CommunicatorMock communicator;
    DataCollection collection;

    setUp(() {
      connection = new ConnectionMock();
      idGenerator = new IdGeneratorMock();
      communicator = new CommunicatorMock();
      collection = new DataCollection();
    });

    tearDown(() {
      months.dispose();
    });

    test("assign id to data.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      months = new Subscription.config('months', collection, connection,
          communicator, 'author', idGenerator);
      january = new Data.from({'name': 'January', 'order': 1});

      // when
      months.start();
      months.collection.add(january);

      // then
      expect(months.collection.first['_id'], equals('prefix-1'));
    });

    test("handle data response.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      List<Map> data = [{'_id': '21', 'name': 'February', 'order': 2}];
      connection = new ConnectionMock();
      months = new Subscription.config('months', collection, connection,
          communicator, 'author', idGenerator);

      // when
      months.handleData(data);

      // then
      expect(months.collection.length, equals(1));
      expect(months.collection.first.containsValue("February"), isTrue);
    });

    test("handle diff response.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      Map marchMapBefore = {'_id': '31', 'name': 'February', 'order': 3};
      Map marchMapAfter = {'_id': '31', 'name': 'March', 'order': 3,
                           'length': 31};
      Map aprilMap = {'_id': '41', 'name': 'April', 'length': 30};
      january = new Data.from({'_id': '11', 'name': 'January', 'order': 1});
      february = new Data.from(marchMapBefore);
      collection.add(january);
      collection.add(february);
      months = new Subscription.config('months', collection, connection,
          communicator, 'author', idGenerator);
      List<Map> diff = [
        {'action': 'add', 'data': aprilMap},
        {'action': 'change', '_id': '31',
         'data': {'_id': '31', 'name': 'March', 'length': 31}},
        {'action': 'remove', '_id': '11'},
        ];

      // when
      months.handleDiff(diff);
      months.collection.addIndex(['_id']);

      // then
      expect(months.collection.length, equals(2));
      expect(months.collection.findBy('_id', '41').first.toJson(),
          equals(aprilMap));
      expect(months.collection.findBy('_id', '31').first.toJson(),
          equals(marchMapAfter));
    });

    test("send add-request.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      months = new Subscription.config('months', collection, connection,
          communicator, 'author', idGenerator);
      january = new Data.from({'name': 'January', 'order': 1});

      // when
      months.start();
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
      january = new Data.from({'name': 'January', 'order': 1});
      collection.add(january);
      months = new Subscription.config('months', collection, connection,
          communicator, 'author', idGenerator);

      // when
      months.start();
      january.addAll({'length': 31});

      // then
      var request = connection.getLogs().last.args[0]();
      expect(request.type, equals("sync"));
      expect(request.args, equals({"action": "change", "collection": "months",
                                   "data": {'length': 31},
                                   "author": "author"}));
    });

    test("send remove-request.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      january = new Data.from({'_id': '12', 'name': 'January', 'order': 1});
      collection.add(january);
      months = new Subscription.config('months', collection, connection,
          communicator, 'author', idGenerator);

      // when
      months.start();
      months.collection.remove(january);

      // then
      var request = connection.getLogs().first.args[0]();
      expect(request.type, equals("sync"));
      expect(request.args, equals({"action": "remove", "collection": "months",
                                   "_id": "12", "author": "author"}));
    });

  });
}