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
class FunctionMock extends Mock implements Function {}
class SubscriptionMock extends Mock implements Subscription {
  SubscriptionMock(value) {
    this.when(callsTo('get initialSync')).alwaysReturn(new Future.value(value));
  }
}

void main() {
  group("Subscriber", () {
    Subscriber subscriber;
    ConnectionMock connection;
    IdGeneratorMock subscriptionIdGenerator, dataIdGenerator;
    Mock subscriptionFactory;

    setUp(() {
      connection = new ConnectionMock();
      connection.when(callsTo('send', anything))
        .alwaysReturn(new Future.value({'id_prefix': 'prefix'}));
      subscriptionIdGenerator = new IdGeneratorMock();
      dataIdGenerator = new IdGeneratorMock();
      subscriptionFactory = new Mock();
    });

    test('id negotiation.', () {
      // given
      subscriber = new Subscriber.config(connection, dataIdGenerator,
          subscriptionIdGenerator, null);

      // when
      var future = subscriber.init();

      // then
      var sendCalls = connection.getLogs(callsTo('send'));
      sendCalls.verify(happenedOnce);
      ClientRequest request = sendCalls.first.args.first();
      expect(request.args, equals({"action": "get_id_prefix"}));

      return future.then((_) {
        subscriptionIdGenerator
            .getLogs(callsTo('set prefix', 'prefix')).verify(happenedOnce);
        dataIdGenerator
            .getLogs(callsTo('set prefix', 'prefix')).verify(happenedOnce);
      });
    });

    test('obtain prefix from init method argument.', () {
      // given
      subscriber = new Subscriber.config(connection, dataIdGenerator,
          subscriptionIdGenerator, null);

      // when
      var future = subscriber.init("custom prefix");

      // then
      return future.then((_) {
        connection.getLogs().verify(neverHappened);
        print(dataIdGenerator.getLogs());
        subscriptionIdGenerator.getLogs(callsTo('set prefix', 'custom prefix'))
            .verify(happenedOnce);
        dataIdGenerator.getLogs(callsTo('set prefix', 'custom prefix'))
            .verify(happenedOnce);
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

    test('subscribe without proper initialization.', () {
      // given
      subscriber = new Subscriber.config(connection, subscriptionIdGenerator,
          dataIdGenerator, subscriptionFactory);

      // when
      var when = () {
        subscriber.subscribe("collection");
      };

      // then
      expect(when, throwsStateError);
    });

  });


  group("Subscription", () {
    ConnectionMock connection;
    IdGeneratorMock idGenerator;
    Subscription months;
    Data january, february;
    CommunicatorMock communicator;
    DataCollection collection;

    Function listenersAreOn = () {
      january = new Data.from({'name': 'January', 'order': 1});
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-123');
      months.collection.add(january);
      bool idWasGenerated = months.collection.first['_id'] == ('prefix-123');
      bool onBeforeChangeIsOn;
      var sendCall = connection.getLogs().last;
      if(sendCall == null) {
        onBeforeChangeIsOn = false;
      } else {
        LogEntry log = connection.getLogs().last;
        var request = log.args[0]();
        onBeforeChangeIsOn = log.methodName == 'send' &&
            request.args['data']['_id'] == 'prefix-123';
      }
      if(idWasGenerated && onBeforeChangeIsOn) {
        return true;
      } else if (!idWasGenerated && !onBeforeChangeIsOn) {
        return false;
      } else {
        throw new Exception('Inconsistent state of listeners!');
      }
    };

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
      handleData(data, months.collection, 'author');

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
      handleDiff(diff, months.collection, 'author');
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
      january = new Data.from({'_id': '11', 'name': 'January', 'order': 1});
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
                                   "_id": "11", "change": {'length': 31},
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

    test("start.", () {
      // given
      months = new Subscription.config('months', collection, connection,
          communicator, 'author', idGenerator);

      // when
      months.start();

      // then
      communicator.getLogs(callsTo('start')).verify(happenedOnce);
      expect(listenersAreOn(), isTrue);
    });

    test("restart.", () {
      // given
      months = new Subscription.config('months', collection, connection,
          communicator, 'author', idGenerator);

      // when
      months.restart();

      // then
      communicator.getLogs(callsTo('stop')).verify(happenedOnce);
      communicator.getLogs(callsTo('start')).verify(happenedOnce);
      expect(communicator.getLogs().last.methodName, equals('start'));
      expect(listenersAreOn(), isTrue);
    });

    test("dispose.", () {
      // given
      months = new Subscription.config('months', collection, connection,
          communicator, 'author', idGenerator);

      // when
      months.dispose();

      // then
      communicator.getLogs(callsTo('stop')).verify(happenedOnce);
      expect(listenersAreOn(), isFalse);
    });

    test("wait.", () {
      Subscription s0 = new SubscriptionMock('value0');
      Subscription s1 = new SubscriptionMock('value1');
      Subscription s2 = new SubscriptionMock('value2');
      Subscription.wait([s0, s1, s2]).then((valueList) {
        expect(valueList[0], equals('value0'));
        expect(valueList[1], equals('value1'));
        expect(valueList[2], equals('value2'));
      });

    });
  });

  group("Communicator", () {
    ConnectionMock connection;
    FunctionMock handleData;
    FunctionMock handleDiff;
    Communicator defaultCommunicator;
    Communicator diffCommunicator = new Communicator(connection, 'months',
        handleData, handleDiff, 'diff');
    Communicator dataCommunicator = new Communicator(connection, 'months',
        handleData, handleDiff, 'data');
    Map data, diff, emptyDiff;

    setUp(() {
      connection = new ConnectionMock();
      handleData = new FunctionMock();
      handleDiff = new FunctionMock();
      data = {'data': 'some_data', 'version': 12};
      diff = {'diff': [{'version': 13}, {'version': 14}]};
      emptyDiff = {'diff': []};
    });

    test("get_data sent after start.", () {
      // given
      connection.when(callsTo('send')).thenCall((_) {
        defaultCommunicator.stop();
        return new Future.value(data);
      });
      handleData = new FunctionMock();
      defaultCommunicator = new Communicator(connection, 'months', handleData,
          handleDiff);

      // when
      defaultCommunicator.start();

      // then
      var request = connection.getLogs().first.args.first();
      expect(request.type, equals('sync'));
      expect(request.args, equals({'action': 'get_data',
        'collection': 'months'}));
    });

    test("handleData called properly.", () {
      // given
      connection.when(callsTo('send')).thenCall((_) {
        defaultCommunicator.stop();
        return new Future.value(data);
      });
      defaultCommunicator = new Communicator(connection, 'months', handleData,
          handleDiff);

      // when
      defaultCommunicator.start();

      // then
      return new Future.delayed(new Duration(milliseconds: 100), () {
        expect(handleData.getLogs(callsTo('call')).first.args.first,
            equals('some_data'));
      });
    });

    test("get_diff sent with proper version number.", () {
      // given
      connection.when(callsTo('send')).thenCall((_) {
        return new Future.value(data);
      }).thenCall((_) {
        defaultCommunicator.stop();
        return new Future.value(emptyDiff);
      });
      defaultCommunicator = new Communicator(connection, 'months', handleData,
          handleDiff);

      // when
      defaultCommunicator.start();

      // then
      return new Future.delayed(new Duration(milliseconds: 100), () {
        var diffRequest = connection.getLogs().last.args.first();
        expect(diffRequest.type, equals('sync'));
        expect(diffRequest.args, equals({'action': 'get_diff',
          'collection': 'months', 'version': 12}));
      });
    });

    test("handleDiff called properly.", () {
      // given
      connection.when(callsTo('send')).thenCall((_) {
        return new Future.value(data);
      }).thenCall((_) {
        defaultCommunicator.stop();
        return new Future.value(diff);
      });
      defaultCommunicator = new Communicator(connection, 'months', handleData,
          handleDiff);

      // when
      defaultCommunicator.start();

      // then
      return new Future.delayed(new Duration(milliseconds: 100), () {
        expect(handleDiff.getLogs(callsTo('call')).first.args.first,
            equals([{'version': 13}, {'version': 14}]));
      });
    });
  });
}
