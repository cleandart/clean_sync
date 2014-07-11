// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library client_test;

import 'package:unittest/unittest.dart';
import 'package:mock/mock.dart';
import 'package:clean_sync/client.dart';
import 'package:clean_sync/id_generator.dart';
import 'package:clean_ajax/client.dart';
import 'package:clean_ajax/common.dart';
import 'package:clean_data/clean_data.dart';
import 'package:useful/useful.dart';
import 'dart:async';

class BareConnectionMock extends Mock implements Connection {}

var lastRequest;

class ConnectionMock extends Mock implements Connection {
  ConnectionMock() {
    when(callsTo('send')).alwaysCall((requestFactory) {
      lastRequest = requestFactory();
      switch (lastRequest.args['action']) {
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

class TransactorMock extends Mock implements Transactor {}

void main(){
  unittestConfiguration.timeout = new Duration(seconds: 5);
  setupDefaultLogHandler();
  run();
}


void run() {
  group("Subscriber", () {
    Subscriber subscriber;
    ConnectionMock connection;
    DataReference updateLock = new DataReference(false);
    IdGeneratorMock subscriptionIdGenerator, dataIdGenerator;
    Mock subscriptionFactory;
    Mock transactorFactory;

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
      subscriber = new Subscriber.config(connection, dataIdGenerator, null, null, updateLock);

      // when
      var future = subscriber.init();

      // then
      var sendCalls = connection.getLogs(callsTo('send'));
      sendCalls.verify(happenedOnce);
      ClientRequest request = sendCalls.first.args.first();
      expect(request.args, equals({"action": "get_id_prefix"}));

      return future.then((_) {
        dataIdGenerator
            .getLogs(callsTo('set prefix', 'prefix')).verify(happenedOnce);
      });
    });

    test('obtain prefix from init method argument.', () {
      // given
      subscriber = new Subscriber.config(connection, dataIdGenerator, null, null, updateLock);

      // when
      var future = subscriber.init("custom prefix");

      // then
      return future.then((_) {
        connection.getLogs().verify(neverHappened);
        dataIdGenerator.getLogs(callsTo('set prefix', 'custom prefix'))
            .verify(happenedOnce);
      });
    });

    test('subscribe when _id_prefix was not obtained.', () {
      // given
      subscriber = new Subscriber.config(connection, subscriptionIdGenerator, subscriptionFactory, transactorFactory, updateLock);

      // then
      expect(()=> subscriber.subscribe("months","months"),
          throwsA(new isInstanceOf<MissingIdPrefixException>
            ("MissingIdPrefixException")));
    });

    skip_test('subscribe when _id_prefix was obtained.', () {
      // given
      Map args = {'key1': 'val1'};
      subscriptionIdGenerator.when(callsTo('next')).alwaysReturn('prefix-1');

      subscriber = new Subscriber.config(connection, dataIdGenerator, subscriptionFactory, transactorFactory, updateLock);

      // when
      var future = subscriber.init().then((_) {
        subscriber.subscribe("months","months");
      });

      return future.then((_) {
        expect(subscriptionFactory.getLogs().first.args,
            equals(['months', connection, new Transactor(null,null,null,null), updateLock]));
      });
    });

    test('subscribe without proper initialization.', () {
      // given
      subscriber = new Subscriber.config(connection, subscriptionIdGenerator, subscriptionFactory, transactorFactory, updateLock);

      // when
      var when = () {
        subscriber.subscribe("collection","collection");
      };

      // then
      expect(when, throwsStateError);
    });

    test('dump subscriber', () {
      Map args = {'key': 'val', 'another key':'yet another value'};
      subscriber = new Subscriber(connection);
      subscriber.init().then((_) {
        subscriber.subscribe("someResource","someCollection")
          ..collection.addAll(['one','2',{},'that was a map',[],'and that a list'])
          ..args = args;
        subscriber.subscribe("anotherResource", "anotherCollection")
          ..collection.addAll(['first',2,3,{'fourth':'value'}])
          ..args = {'first key': 1, 'second': ['it','is','a','list']};
        expect(subscriber.dump(true),
          equals( 'Subscription(ver: 0)\n'+
                  'Resource name: someResource \n' +
                  'Collection name: someCollection \n'+
                  'Args: {key: val, another key: yet another value} \n'+
                  'Initial sync completed? false\n'+
                  'Data: [one, 2, {}, that was a map, [], and that a list] \n\n'+
                  'Subscription(ver: 0)\n'+
                  'Resource name: anotherResource \n' +
                  'Collection name: anotherCollection \n'+
                  'Args: {first key: 1, second: [it, is, a, list]} \n'+
                  'Initial sync completed? false\n'+
                  'Data: [first, 2, 3, {fourth: value}] \n\n'
                ));
      });
    });

  });


  group("Subscription", () {
    ConnectionMock connection;
    IdGeneratorMock idGenerator;
    Subscription months;
    Transactor transactor;
    DataMap january, february;
    DataSet collection;
    DataReference updateLock;
    FunctionMock mockHandleData;
    FunctionMock mockHandleDiff;
    Function createSubscriptionStub;

    Function listenersAreOn = () {
      january = new DataMap.from({'name': 'January', 'order': 1});
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-123');
      lastRequest = null;
      months.collection.add(january);
      // ID is generated and changes are propagated to server when listeners are
      // on.
      bool idWasGenerated = months.collection.first['_id'] == ('prefix-123');
      return new Future.delayed(new Duration(milliseconds: 100), (){
        bool changeWasSent;
        var sendCall = connection.getLogs().last;
        if(sendCall == null) {
          changeWasSent = false;
        } else {
          LogEntry log = connection.getLogs().last;
          print(lastRequest.args);
          changeWasSent = log.methodName == 'send' &&
              lastRequest.args['args']["data"][0]["_id"] == 'prefix-123';
        }
        if(idWasGenerated && changeWasSent) {
          return true;
        } else if (!idWasGenerated && !changeWasSent) {
          return false;
        } else {
          throw new Exception('Inconsistent state of listeners!');
        }
      });
    };

    setUp(() {
      connection = new ConnectionMock();
      idGenerator = new IdGeneratorMock();
      mockHandleData = new FunctionMock();
      mockHandleDiff = new FunctionMock();
      updateLock = new DataReference(false);
      transactor = new Transactor(connection,updateLock, 'author', idGenerator);
      collection = new DataSet();
      collection.addIndex(['_id']);
      months = new Subscription.config('monthsResource','months', collection, connection, idGenerator,
        transactor, mockHandleData, mockHandleDiff, false, updateLock);
      months.initialSync.catchError((e){});

      createSubscriptionStub = (collection){
        return new Subscription.config('monthsResource','months', collection, connection, idGenerator,
        transactor, mockHandleData, mockHandleDiff, false, updateLock);
      };

    });

    tearDown(() {
      if (months != null) return months.dispose();
    });

    test("assign id to data.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      january = new DataMap.from({'name': 'January', 'order': 1});

      // when
      months.setupListeners();
      months.collection.add(january);

      // then
      expect(months.collection.first['_id'], equals('prefix-1'));
    });

    test("handle data response.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      List<Map> data = [{'_id': '21', 'name': 'February', 'order': 2}];
      connection = new ConnectionMock();

      // when
      handleData(data, months);

      // then
      expect(months.collection.length, equals(1));
      expect(months.collection.first.containsValue("February"), isTrue);
    });

    test("initial sync.", () {
      // given
      connection = new ConnectionMock();
      Subscription _months = createSubscriptionStub(collection);

      // then
      _months.initialSync.catchError(expectAsync((e){}));

      // when
      return _months.dispose();

    });

    skip_test("modifiedItems", () {
      var _connection = new BareConnectionMock();
      var elem = new DataMap.from({'_id': '1', 'name': 'arthur'});
      _connection.when(callsTo('send')).alwaysCall((requestFactory) {
        var request = requestFactory();
        switch (request.args['action']) {
          case ('get_diff'): return new Future.delayed(
              new Duration(milliseconds: 100), () => {'diff': [{
            'action' : 'change',
            'author' : 'ford',
            '_id' : '1',
            'data' : {
              '_id': '1',
              'name': 'ford'
            }
          }]});
          case ('jsonChange'): {
            return new Future.delayed(new Duration(milliseconds: 200),
               () => new Future.value(1));
          }
          default: return new Future.value(1);
        }
      });

      collection.add(elem);

      Subscription subs = new Subscription.config('collectionResource','collection',
          collection, _connection, idGenerator, transactor, mockHandleData, mockHandleDiff, false, updateLock);

      subs.setupListeners();

      _createDiffRequest() => new ClientRequest("sync", {
        "action" : "get_diff",
        "collection" : 'collection',
        "version" : 0
      });

      _connection.send(_createDiffRequest).then((val){
        handleDiff(val['diff'], subs);
      });

      elem['name'] = 'trillian';
      elem['name'] = 'tricia';

      return Future.wait([
        new Future.delayed(new Duration(milliseconds: 300), (){
          expect(elem['name'], equals('tricia'));
        }),

        new Future.delayed(new Duration(milliseconds: 300), (){
          expect(elem['name'], equals('tricia'));
        })
      ]);

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
      List<Map> diff = [
        {'action': 'add', 'data': aprilMap},
        {'action': 'change', '_id': '31',
         'data': new DataMap.from(marchMapAfter)},
        {'action': 'remove', '_id': '11'},
        ];

      Subscription _months = new Subscription.config('months', collection, connection,
          'author', idGenerator, mockHandleData, mockHandleDiff, false);
      _months.initialSync.catchError((e){});


      // when
      handleDiff(diff, _months, 'author');
      _months.collection.addIndex(['_id']);

      // then
      expect(_months.collection.length, equals(2));
      expect(_months.collection.findBy('_id', '41').first.toString(),
          equals(aprilMap.toString()));
      expect(_months.collection.findBy('_id', '31').first.toString(),
          equals(marchMapAfter.toString()));
      return _months.dispose();
    });

    skip_test("handle diff response complex", (){
      DataMap guybrush = new DataMap.from({'name' : 'Guybrush'});
      DataReference guybrushNameRef = guybrush.ref('name');
      DataMap lechuck = new DataMap.from({'name' : 'LeChuck'});
      DataMap mi = new DataMap.from({'_id' : '1', 'good': guybrush, 'evil': lechuck});
      DataMap loom = new DataMap.from({'_id': '3', 'game': 'loom'});
      DataMap summary = new DataMap.from({'_id' : '2', 'characters': new DataList.from([new DataMap.from(guybrush)])});
      DataSet games = new DataSet.from([mi, summary, loom]);
      games.addIndex(['_id']);

      Subscription gamesSubs  = createSubscriptionStub(games);
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

      handleDiff(diff, gamesSubs);
      guybrush.onChange.listen(expectAsync((change){
        expect(guybrushNameRef, equals(guybrush.ref('name')));
        expect(change.equals(new ChangeSet(
            {'name': new Change('Guybrush', 'Guybrush Threepwood')}
        )), isTrue);
      }));
      lechuck.onChange.listen((change){
        expect(true, isFalse);
      });
      summary.onChange.listen(expectAsync((change){
        var added = summary['characters'][1];
        expect(added is DataMap, isTrue);
        expect(change.equals(
            new ChangeSet({
              'characters': new ChangeSet({1: new Change(undefined, added)})
            })
        ), isTrue);
      }));
      games.onChange.listen(expectAsync((ChangeSet change){
        expect(change.addedItems.length, equals(1));
        expect(change.addedItems.first['game'], equals('grim fandango'));
        expect(change.removedItems.length, equals(1));
        expect(change.removedItems.first['game'], equals('loom'));
      }));

    });

    test("send add-request.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      january = new DataMap.from({'name': 'January', 'order': 1});

      // when
      months.setupListeners();
      lastRequest = null;
      months.collection.add(january);

      // then
      return new Future.delayed(new Duration(milliseconds: 100), (){
        expect(lastRequest, isNotNull);
        expect(lastRequest.type, equals("sync-operation"));
        expect(lastRequest.args, equals({
          "operation": "addAll",
          "colls": ["months"],
          "args": {"data" : [january]},
          "author": "author",
          "clientVersion": "prefix-1",
          "docs" : []
         }));
      });
    });

    test("send change-request.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      january = new DataMap.from({'_id': '11', 'name': 'January', 'order': 1});
      collection = new DataSet.from([january]);

      Subscription _months = createSubscriptionStub(collection);

      // when
      _months.setupListeners();
      lastRequest = null;
      january.addAll({'length': 31});

      // then
      return new Future.delayed(new Duration(milliseconds: 100), (){
        var request = connection.getLogs().last.args[0]();
        expect(lastRequest.type, equals("sync-operation"));
        expect(slice(lastRequest.args, ['operation', 'author', 'args']),
            equals({"operation": "change",
                   "args": {'length': [CLEAN_UNDEFINED, 31]}, "author": "author"}));
      });
    });

    test("send remove-request.", () {
      // given
      idGenerator.when(callsTo('next')).alwaysReturn('prefix-1');
      january = new DataMap.from({'_id': '12', 'name': 'January', 'order': 1});
      collection.add(january);

      Subscription _months = createSubscriptionStub(collection);

      // when
      _months.setupListeners();
      lastRequest = null;
      _months.collection.remove(january);

      // then
      return new Future.delayed(new Duration(milliseconds: 100), (){
        expect(lastRequest.type, equals("sync-operation"));
        expect(slice(lastRequest.args, ['operation', 'colls', 'author', 'args']),
            equals({
          "operation": "removeAll",
          "colls": ["months"],
          "author": "author",
          "args": {"ids" : ["12"]}
        }));
      });
    });

    test("setupListeners", () {

      // when
      months.setupListeners();

      // then
//      var request = connection.getLogs().first.args.first;
      return listenersAreOn().then((res){
        expect(res, isTrue);
      });
    });


    test("dispose.", () {
      // when
      months.dispose();

      // then
      return listenersAreOn().then((res){
        expect(res, isFalse);
      });
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
