library collection_modification_test;

import "package:unittest/unittest.dart";
import "package:clean_sync/server.dart";
import "dart:async";
import './mongo_provider_test.dart';
import 'package:clean_sync/client.dart';
import 'package:unittest/mock.dart';
import 'package:clean_ajax/client.dart';
import 'package:clean_ajax/client_backend.dart';
import 'package:clean_ajax/server.dart';
import 'package:clean_data/clean_data.dart';
import 'package:logging/logging.dart';

class BareConnectionMock extends Mock implements Connection {}
class IdGeneratorMock extends Mock implements IdGenerator {}

main(){
  unittestConfiguration.timeout = null;
  hierarchicalLoggingEnabled = true;
  Logger.root.level = Level.WARNING;
  (new Logger('clean_sync')).level = Level.WARNING;
  Logger.root.onRecord.listen((LogRecord rec) {
    print('${rec.loggerName} ${rec.level} ${rec.message} ${rec.error} ${rec.stackTrace}');
  });
  run();
}

run() {

  MongoDatabase mongodb;
  DataSet colAll;
  DataSet colAll2;
  DataSet colA;
  DataSet colAa;
  DataSet colMapped;

  Connection connection;
  Subscription subAll;
  Subscription subAll2;
  Subscription subA;
  Subscription subAa;
  Subscription subMapped;

  DataMap data1;
  DataMap data2;
  DataMap dataA;

  Publisher pub;


  setUp((){
    mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest');

    return Future.wait(mongodb.init)
    .then((_) => mongodb.dropCollection('random'))
    .then((_) => mongodb.removeLocks()).then((_){
        cacheFactory() => new Cache(new Duration(milliseconds: 10), 10000);

        pub = new Publisher();
        pub.publish('a', (_) {
          return mongodb.collection("random").find({});
        }, cacheFactory: cacheFactory);

        pub.publish('b', (_) {
          return mongodb.collection("random").find({'a': 'hello'});
        }, cacheFactory: cacheFactory);

        pub.publish('c', (_) {
          return mongodb.collection("random").find({'a.a': 'hello'});
        }, cacheFactory: cacheFactory);

        pub.publish('mapped', (_) {
          return mongodb.collection("random").find({});
        }, projection: (Map elem){
          elem.remove('a');
          elem['aa'] = 'it works gr8';
        }, cacheFactory: cacheFactory);


        MultiRequestHandler requestHandler = new MultiRequestHandler();
        requestHandler.registerDefaultHandler(pub.handleSyncRequest);
        connection = createLoopBackConnection(requestHandler);

        subAll = new Subscription('a', connection, 'author1', new IdGenerator('a'), {});
        colAll = subAll.collection;
        subAll2 = new Subscription('a', connection, 'author2', new IdGenerator('b'), {});
        colAll2 = subAll2.collection;
        subA = new Subscription('b', connection, 'author3', new IdGenerator('c'), {});
        colA = subA.collection;
        subAa = new Subscription('c', connection, 'author4', new IdGenerator('d'), {});
        colAa = subAa.collection;
        subMapped = new Subscription('mapped', connection, 'author5', new IdGenerator('e'), {});
        colMapped = subMapped.collection;

        data1 = new DataMap.from({'_id': '0', 'colAll' : 'added from colAll'});
        data2 = new DataMap.from({'_id': '1', 'colAll2': 'added from colAll2'});
        dataA = new DataMap.from({'_id': '2', 'a': 'hello'});
    });
  });

  tearDown(() {
    List itemsToClose = [
      subAll,
      subAll2,
      subA,
      subAa,
      subMapped
    ];

    return Future.forEach(itemsToClose, (item) {
      return item.close();
    }).then((_) => mongodb.close())
    .then((_) => pub.close());
  });

  executeSubscriptionActions(List actions) {
    return
    mongodb.dropCollection('random').then((_) =>
    mongodb.removeLocks()).then((_) =>
    subAll.initialSync).then((_) =>
    subAll2.initialSync).then((_) =>
    subA.initialSync).then((_) =>
    subAa.initialSync).then((_) =>
    subMapped.initialSync).then((_) =>
    Future.forEach(actions, (action) {
      action();
      return new Future.delayed(new Duration(milliseconds: 200));
    }));
  }

  test('data added to the set is not cloned, if it is already DataMap', () {
    DataMap data = new DataMap.from({'_id': '0', 'a' : 'aa'});

    List actions = [
      () => colAll.add(data),
      () => expect(data == colAll.first, isTrue)
    ];

    return executeSubscriptionActions(actions);

  });

  test('test collection add', () {
    List actions = [
      () => colAll.add(data1),
      () => expect(colAll2, unorderedEquals([data1])),
      () {colAll2.add(data2); colAll.add(dataA);},
      () => expect(colAll, unorderedEquals(colAll2)),
    ];

    return executeSubscriptionActions(actions);

  });

  test('test collection change', () {
    List actions = [
      () => colAll.add(data1),
      () => colAll2.first['colAll'] = 'changed from colAll2',
      () => expect(colAll, unorderedEquals([
        {'_id' : '0', 'colAll' : 'changed from colAll2'}
      ])),
    ];

    return executeSubscriptionActions(actions);

  });

  test('test collection remove', () {
    List actions = [
      () => colAll2.add(data1),
      () => colAll2.removeBy('_id', '0'),
      () => expect(colAll.isEmpty, isTrue),
    ];

    return executeSubscriptionActions(actions);

  });

  test('locking working properly', (){
    preventUpdate(Subscription subscription){
      return (event) => expect(subscription.updateLock, isTrue);
    }
    colAll2.onChangeSync.listen(preventUpdate(subAll2));
    colA.onChangeSync.listen(preventUpdate(subA));
    colAa.onChangeSync.listen(preventUpdate(subAa));
    colMapped.onChangeSync.listen(preventUpdate(subMapped));
    List actions = [
      () { colAll.add(data1);
           colAll.removeBy('_id', '0');
           colAll.add(data1);
           data1['name'] = 'phero';
           data1['nums'] = [];
           data1['nums'].add(1);
           data1['nums'].remove(1);
      },
    ];

    return executeSubscriptionActions(actions);
  });

  test('test collection filtered add', () {
    List actions = [
      () => colAll.add(data1),
      () => expect(colA.isEmpty, isTrue),
      () => colAll.add(dataA),
      () => expect(colA, unorderedEquals([dataA])),
    ];

    return executeSubscriptionActions(actions);

  });

  test('test collection filtered change', () {
    List actions = [
      () => colAll.add(dataA),
      () => expect(colA, unorderedEquals([{'_id' : '2', 'a': 'hello'}])),
      () => expect(colAa, unorderedEquals([])),
      () => dataA['a'] = {'a': 'hello'},
      () => expect(colAa, unorderedEquals([{'_id' : '2', 'a' : {'a': 'hello'}}])),
      () => expect(colA, unorderedEquals([])),
    ];

    return executeSubscriptionActions(actions);

  });


  test('test remove from filtered collection by changing element', () {
    List actions = [
      () => colA.add({'_id': '1', 'a': 'hello', 'b': 'world'}),
      () => colA.first['a'] = 'chello',
      () => expect(colA.isEmpty, isTrue),
      () => colAa.add({'_id': '2', 'a': {'a': 'hello'}, 'b': 'world'}),
      () => colAa.first['a'].remove('a'),
      () => expect(colAa.isEmpty, isTrue),
    ];
    return executeSubscriptionActions(actions);
  });


  test('test collection filtered remove', () {
    List actions = [
      () => colA.add(dataA),
      () => colAll.removeBy('_id', '2'),
      () => expect(colA.isEmpty, isTrue),
      () => expect(colAll.isEmpty, isTrue),
    ];

    return executeSubscriptionActions(actions);

  });

  test('test collection mapped', () {
    Subscription newSub;
    List actions = [
      () => colAll.add({'a': 1, 'b': 2}),
      () => expect(colMapped, equals([{'b': 2, '_id': 'a-1', 'aa': 'it works gr8'}])),
      () => colAll.add({'a': {}, 'b': []}),
      () => newSub = new Subscription(subMapped.collectionName, connection, 'dummyAuthor', new IdGeneratorMock()),
      () => expect(colMapped, unorderedEquals(newSub.collection)),
    ];

    return executeSubscriptionActions(actions);

  });

  test('test data list manipulation', () {
    Subscription newSub;
    DataMap morders = new DataMap();
    DataList orders = new DataList();
    colAll2.onChangeSync.listen((event){
      expect(subAll2.updateLock, isTrue);
    });
    List actions = [
      () => colAll.add({'order' : orders}),
      () {orders.add(1); orders.add(2); orders.add(3); orders.add(4);},
      () {orders.remove(2); orders.remove(3); orders.remove(4);},
      () => expect(orders, equals([1])),
    ];

    return executeSubscriptionActions(actions);

  });

  test('add-remove-add', () {
    Subscription newSub;
    List actions = [
      () {colAll.add(data1); colAll.remove(data1); colAll.add(data1);},
      () => expect(colAll, unorderedEquals([data1])),
      () {colAll.remove(data1); colAll.add(data1);},
      () => expect(colAll, unorderedEquals([data1])),
    ];

    return executeSubscriptionActions(actions);

  });




}
