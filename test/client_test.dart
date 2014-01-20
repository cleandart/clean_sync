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

class BareConnectionMock extends Mock implements Connection {}

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
    FunctionMock mockHandleData;
    FunctionMock mockHandleDiff;

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
      mockHandleData = new FunctionMock();
      mockHandleDiff = new FunctionMock();
      collection = new DataSet();
    });

    tearDown(() {
      if (months != null) months.dispose();
    });

    test("assign id to data.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      months = new Subscription.config('months', collection, connection,
          'author', idGenerator, mockHandleData, mockHandleDiff, false);
      january = new DataMap.from({'name': 'January', 'order': 1});

      // when
      months.setupListeners();
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
        'author', idGenerator, mockHandleData, mockHandleDiff, false);

      // when
      handleData(data, months.collection, 'author');

      // then
      expect(months.collection.length, equals(1));
      expect(months.collection.first.containsValue("February"), isTrue);
    });

    solo_test("tokens", () {


      var connection = new BareConnectionMock();
      var elem = new DataMap.from({'_id': 1, 'name': 'johny'});

      connection.when(callsTo('send')).alwaysCall((requestFactory) {
        switch (requestFactory().args['action']) {
          case ('get_diff'): return new Future.value({'diff': null});
          case ('get_data'): return new Future.value({'version': 2, 'data': [elem]});
          case ('get_id_prefix'): return new Future.value({'id_prefix': 'prefix'});
          default: return new Future.value(null);
        }
      });

      Subscription subs  = new Subscription.config('collection', collection, connection,
          'author', idGenerator, mockHandleData, mockHandleDiff, false);

      _createDataRequest() => new ClientRequest("sync", {
        "action" : "get_data",
        "collection" : 'collection'
      });

      connection.send(_createDataRequest).then((val){
        print(val);
        handleData(val['data'], collection, 'author');
        print(collection);
      });
    });



    skip_test("handle diff response.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      DataMap marchMapBefore = new DataMap.from({'_id': '31', 'name': 'February', 'order': 3});
      DataMap marchMapAfter = new DataMap.from({'_id': '31', 'name': 'March', 'order': 3,
                           'length': 31});
      DataMap aprilMap = new DataMap.from({'_id': '41', 'name': 'April', 'length': 30});
      january = new DataMap.from({'_id': '11', 'name': 'January', 'order': 1});
      february = new DataMap.from(marchMapBefore);
      collection.add(january);
      collection.add(february);
      months = new Subscription.config('months', collection, connection,
          'author', idGenerator, mockHandleData, mockHandleDiff, false);
      List<Map> diff = [
        {'action': 'add', 'data': aprilMap},
        {'action': 'change', '_id': '31',
         'data': new DataMap.from(marchMapAfter)},
        {'action': 'remove', '_id': '11'},
        ];

      // when
      handleDiff(diff, months, 'author');
      months.collection.addIndex(['_id']);

      // then
      expect(months.collection.length, equals(2));
      expect(months.collection.findBy('_id', '41').first.toString(),
          equals(aprilMap.toString()));
      expect(months.collection.findBy('_id', '31').first.toString(),
          equals(marchMapAfter.toString()));
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
          'author', idGenerator, mockHandleData, mockHandleDiff, false);
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
          'author', idGenerator, mockHandleData, mockHandleDiff, false);
      january = new DataMap.from({'name': 'January', 'order': 1});

      // when
      months.setupListeners();
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
          'author', idGenerator, mockHandleData, mockHandleDiff, false);

      // when
      months.setupListeners();
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
          'author', idGenerator, mockHandleData, mockHandleDiff, false);

      // when
      months.setupListeners();
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
          'author', idGenerator, mockHandleData, mockHandleDiff, false);

      // when
      months.setupListeners();

      // then
//      var request = connection.getLogs().first.args.first;
      expect(listenersAreOn(), isTrue);
    });

//    test("restart.", () {
//      // given
//      months = new Subscription.config('months', collection, connection,
//          'author', idGenerator, mockHandleData, mockHandleDiff, false);
//
//      // when
//      months.restart();
//
//      // then
//      // TODO
//      expect(listenersAreOn(), isTrue);
//    });

    test("dispose.", () {
      // given
      months = new Subscription.config('months', collection, connection,
          'author', idGenerator, mockHandleData, mockHandleDiff, false);

      // when
      months.dispose();

      // then
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
}
