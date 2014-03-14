library collection_modification_test;

import "package:unittest/unittest.dart";
import "package:clean_sync/server.dart";
import "dart:async";
import 'package:clean_sync/client.dart';
import 'package:clean_ajax/client.dart';
import 'package:clean_ajax/client_backend.dart';
import 'package:clean_ajax/server.dart';
import 'package:clean_data/clean_data.dart';
import 'package:logging/logging.dart';
import 'package:useful/useful.dart';
import './client_test.dart';


main(){
  hierarchicalLoggingEnabled = true;
  unittestConfiguration.timeout = null;
//  (new Logger('clean_sync')).level = Level.FINEST;
  setupDefaultLogHandler();
  run();
}

run() {

  MongoDatabase mongodb;
  DataSet colAll;
  DataSet colAll2;
  DataSet colA;
  DataSet colAa;
  DataSet colArgs;

  Connection connection;
  Subscription subAll;
  Subscription subAll2;
  Subscription subA;
  Subscription subAa;
  Subscription subArgs;

  DataMap data1;
  DataMap data2;
  DataMap dataA;

  Publisher pub;


  setUp((){
    Cache cache = new Cache(new Duration(milliseconds: 10), 10000);
    mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest', cache: cache);

    return Future.wait(mongodb.init)
    .then((_) => mongodb.dropCollection('random'))
    .then((_) => mongodb.removeLocks()).then((_){

        pub = new Publisher();
        pub.publish('a', (_) {
          return mongodb.collection("random").find({});
        });

        pub.publish('b', (_) {
          return mongodb.collection("random").find({'a': 'hello'});
        });

        pub.publish('c', (_) {
          return mongodb.collection("random").find({'a.a': 'hello'});
        });

        pub.publish('withArgs', (args) {
          return mongodb.collection("random").find({'a': args['name']});
        });

        pub.publish('mapped_pos', (_) {
          return mongodb.collection("random").find({'b': 3}).fields(['a']);
        });

        pub.publish('mapped_neg', (_) {
          return mongodb.collection("random").find({'b': 3}).excludeFields(['a']);
        });

        MultiRequestHandler requestHandler = new MultiRequestHandler();
        requestHandler.registerDefaultHandler(pub.handleSyncRequest);
        connection = createLoopBackConnection(requestHandler);

        subAll = new Subscription('a', connection, 'author_sub_all', new IdGenerator('a'), {});
        colAll = subAll.collection;
        subAll2 = new Subscription('a', connection, 'author_sub_all2', new IdGenerator('b'), {});
        colAll2 = subAll2.collection;
        subA = new Subscription('b', connection, 'author_sub_a', new IdGenerator('c'), {});
        colA = subA.collection;
        subAa = new Subscription('c', connection, 'author_sub_aa', new IdGenerator('d'), {});
        colAa = subAa.collection;
        subArgs = new Subscription('withArgs', connection, 'author_sub_args', new IdGenerator('d'),
            {'name': 'aa'});
        colArgs = subArgs.collection;

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
      subArgs,
    ];

    return Future.forEach(itemsToClose, (item) {
      return item.close();
    }).then((_) => mongodb.close());
  });

  executeSubscriptionActions(List actions) {
    return
    mongodb.dropCollection('random').then((_) =>
    mongodb.removeLocks()).then((_) =>
    subAll.initialSync).then((_) =>
    subAll2.initialSync).then((_) =>
    subA.initialSync).then((_) =>
    subAa.initialSync).then((_) =>
    subArgs.initialSync).then((_) =>
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

  test('test collection fields', () {
    Subscription newSub;
    Subscription subMapped = new Subscription('mapped_pos', connection, 'author5', new IdGenerator('e'), {});
    DataSet colMapped = subMapped.collection;

    List actions = [
      () => colAll.add({'a': 1, 'b': 3, 'c': 2}),
      () => colAll.add({'a': 2, 'b': 4, 'c': 2}),
      () => expect(colMapped, equals([{'a': 1, '_id': 'a-1'}])),
      () => newSub = new Subscription(subMapped.collectionName, connection, 'dummyAuthor', new IdGeneratorMock()),
      () => expect(colMapped, unorderedEquals(newSub.collection)),
      () {subMapped.close(); newSub.close();}
    ];

    return executeSubscriptionActions(actions);

  });

  test('test collection excluded fields', () {
    Subscription newSub;
    Subscription subMapped = new Subscription('mapped_neg', connection, 'author5', new IdGenerator('e'), {});
    DataSet colMapped = subMapped.collection;

    List actions = [
      () => colAll.add({'a': 1, 'b': 3, 'c': 2}),
      () => colAll.add({'a': 2, 'b': 4, 'c': 2}),
      () => expect(colMapped, equals([{'b': 3, 'c': 2, '_id': 'a-1'}])),
      () => newSub = new Subscription(subMapped.collectionName, connection, 'dummyAuthor', new IdGeneratorMock()),
      () => expect(colMapped, unorderedEquals(newSub.collection)),
      () {subMapped.close(); newSub.close();}
    ];

    return executeSubscriptionActions(actions);

  });


  test('test data list manipulation', () {
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
    List actions = [
      () {colAll.add(data1); colAll.remove(data1); colAll.add(data1);},
      () => expect(colAll, unorderedEquals([data1])),
      () {colAll.remove(data1); colAll.add(data1);},
      () => expect(colAll, unorderedEquals([data1])),
    ];

    return executeSubscriptionActions(actions);

  });


  test('add-remove-add', () {
    List actions = [
      () {colAll.add(data1); colAll.remove(data1); colAll.add(data1);},
      () => expect(colAll, unorderedEquals([data1])),
      () {colAll.remove(data1); colAll.add(data1);},
      () => expect(colAll, unorderedEquals([data1])),
    ];

    return executeSubscriptionActions(actions);

  });

  test("restart immediately renews initialSync", (){
    return subArgs.initialSync.then((_){
      Future oldinitialSync = subArgs.initialSync;
      subArgs.restart(null).then((_){});
      expect(subArgs.initialSync == oldinitialSync, isFalse);
    });
  });

  test('restart', () {
    List actions = [
      // close unneeded subscriptions to have nicer log
      (){
        subAll2.close();
        subA.close();
        subAa.close();
      },
      () {colAll.addAll([{'a': 'aa'}, {'a': 'bb'}]);},
      () => expect(colArgs, unorderedEquals([{'a': 'aa', '_id': 'a-1'}])),
      () => subArgs.restart({'name': 'bb'}),
      () => expect(colArgs, unorderedEquals([{'a': 'bb', '_id': 'a-2'}])),
      () => colAll.add({'a': 'bb'}),
      () => colAll.add({'a': 'aa'}),
      () => expect(colArgs, unorderedEquals([{'a': 'bb', '_id': 'a-2'}, {'a': 'bb', '_id': 'a-3'}])),
    ];
    return executeSubscriptionActions(actions);
  });

  test('changes immediately between restart are still saved correctly', () {
    List actions = [
      () {colArgs.add({'a': 'aa'}); subArgs.restart({'name': 'bb'});},
      () => expect(colAll, unorderedEquals([{'a': 'aa', '_id': 'd-1'}])),
      () => expect(colArgs.isEmpty, isTrue),
    ];
    return executeSubscriptionActions(actions);
  });

  test('new data are present immediately after initial_sync completes', () {
    List actions = [
      () => colAll.addAll([{'a': 'aa'}, {'a': 'bb'}]),
      () {
        subArgs.restart({'name': 'bb'});
        subArgs.initialSync.then((_){
          expect(colArgs, unorderedEquals([{'a': 'bb', '_id': 'a-2'}]));
        });
      },
    ];
    return executeSubscriptionActions(actions);
  });



}
