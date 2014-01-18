// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library client_test;

import 'package:unittest/unittest.dart';
import 'package:unittest/mock.dart';
import 'package:clean_sync/client.dart';
import 'package:clean_ajax/client.dart';
import 'package:clean_ajax/common.dart';
import 'package:clean_data/clean_data.dart';
import 'dart:async';

class ConnectionMock extends Mock implements Connection {
  ConnectionMock() {
    when(callsTo('send')).alwaysCall((requestFactory) {
      switch (requestFactory().args['action']) {
        case ('get_diff'): return new Future.value({'diff': null});
        case ('get_data'): return new Future.value({'version': 2, 'data': null});
        case ('get_id_prefix'): return new Future.value({'id_prefix': 'prefix'});
        default: return new Future.value(null);
      }
    });
  }
}
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
    DataMap january, february;
    CommunicatorMock communicator;
    DataSet collection;

    Function listenersAreOn = () {
      january = new DataMap.from({'name': 'January', 'order': 1});
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-123');
      months.collection.add(january);
      // ID is generated and changes are propagated to server when listeners are
      // on.
      bool idWasGenerated = months.collection.first['_id'] == ('prefix-123');
      bool changeWasSent;
      var sendCall = connection.getLogs().last;
      if(sendCall == null) {
        changeWasSent = false;
      } else {
        LogEntry log = connection.getLogs().last;
        var request = log.args[0]();
        changeWasSent = log.methodName == 'send' &&
            request.args['data']['_id'] == 'prefix-123';
      }
      if(idWasGenerated && changeWasSent) {
        return true;
      } else if (!idWasGenerated && !changeWasSent) {
        return false;
      } else {
        throw new Exception('Inconsistent state of listeners!');
      }
    };

    setUp(() {
      connection = new ConnectionMock();
      idGenerator = new IdGeneratorMock();
      communicator = new CommunicatorMock();
      collection = new DataSet();
    });

    tearDown(() {
      if (months != null) months.dispose();
    });

    test("assign id to data.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      months = new Subscription.config('months', collection, connection,
          communicator, 'author', idGenerator);
      january = new DataMap.from({'name': 'January', 'order': 1});

      // when
      months.start();
      months.collection.add(january);

      // then
//      expect(months.collection.first['_id'], equals('prefix-1'));
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


    test("handle diff response", (){
      DataMap guybrush = new DataMap.from({'name' : 'Guybrush'});
      DataReference guybrushNameRef = guybrush.ref('name');
      DataMap lechuck = new DataMap.from({'name' : 'LeChuck'});
      DataMap mi = new DataMap.from({'_id' : '1', 'good': guybrush, 'evil': lechuck});
      DataMap loom = new DataMap.from({'_id': '3', 'game': 'loom'});
      DataMap summary = new DataMap.from({'_id' : '2', 'characters': new DataList.from([new DataMap.from(guybrush)])});
      DataSet games = new DataSet.from([mi, summary, loom]);
      Subscription gamesSubs  = new Subscription.config('games', games, connection,
          communicator, 'author', idGenerator);
      List<Map> diff = [
        {'action': 'change', '_id': '1',
          'data': {'_id' : '1',
             'good': {'name': 'Guybrush Threepwood'},
             'evil': {'name': 'LeChuck'}
          }
        },
        {'action': 'change', '_id': '2',
          'data': {'_id' : '2',
             'characters': [new Map.from(guybrush), new Map.from(lechuck)]
          }
        },
        {'action': 'remove', '_id': '3'},
        {'action': 'add', '_id': '4',
          'data': {'_id' : '4', 'game': 'grim fandango'}
        },
      ];

      handleDiff(diff, gamesSubs, 'author');
      guybrush.onChange.listen(expectAsync1((change){
        expect(guybrushNameRef, equals(guybrush.ref('name')));
        expect(change.equals(new ChangeSet(
            {'name': new Change('Guybrush', 'Guybrush Threepwood')}
        )), isTrue);
      }));
      lechuck.onChange.listen((change){
        expect(true, isFalse);
      });
      summary.onChange.listen(expectAsync1((change){
        var added = summary['characters'][1];
        expect(added is DataMap, isTrue);
        expect(change.equals(
            new ChangeSet({
              'characters': new ChangeSet({1: new Change(undefined, added)})
            })
        ), isTrue);
      }));
      games.onChange.listen(expectAsync1((ChangeSet change){
        expect(change.addedItems.length, equals(1));
        expect(change.addedItems.first['game'], equals('grim fandango'));
        expect(change.removedItems.length, equals(1));
        expect(change.removedItems.first['game'], equals('loom'));
      }));

    });

    test("send add-request.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      months = new Subscription.config('months', collection, connection,
          communicator, 'author', idGenerator);
      january = new DataMap.from({'name': 'January', 'order': 1});

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
      january = new DataMap.from({'_id': '11', 'name': 'January', 'order': 1});
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
      january = new DataMap.from({'_id': '12', 'name': 'January', 'order': 1});
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
      connection.resetBehavior();
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
//      // given
      connection.resetBehavior();
      connection.when(callsTo('send')).alwaysCall((_) {
        defaultCommunicator.stop();
        return new Future.value(data);
      });

      defaultCommunicator = new Communicator(connection, 'months', handleData,
          handleDiff);
//
//      // when
      defaultCommunicator.start();

      // then

      return new Future.delayed(new Duration(milliseconds: 100), () {
        expect(handleData.getLogs(callsTo('call')).first.args.first,
            equals('some_data'));

      });
    });

    test("get_diff sent with proper version number.", () {
      // given
      connection.resetBehavior();
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
      connection.resetBehavior();
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
