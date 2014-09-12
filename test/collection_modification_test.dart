library clean_sync.test.collection_modification_test;

import "package:unittest/unittest.dart";
import "dart:async";
import 'package:clean_ajax/client.dart';
import 'package:clean_ajax/client_backend.dart';
import 'package:clean_ajax/server.dart';
import 'package:clean_data/clean_data.dart';
import 'package:logging/logging.dart';
import 'package:useful/useful.dart';
import 'package:clean_sync/client.dart';
import "package:clean_sync/server.dart";
import 'package:clean_sync/transactor_server.dart';
import 'package:clean_sync/id_generator.dart';
import 'package:clean_lock/lock_requestor.dart';


stripIds(Iterable data) => data.map((elem) => new Map.from(elem)..remove('_id')
..remove('__clean_collection'));

allowOperation(ServerOperationCall) => true;

main(){
  hierarchicalLoggingEnabled = true;
  unittestConfiguration.timeout = null;
  (new Logger('clean_sync')).level = Level.WARNING;
  (new Logger('mongo_wrapper_logger')).level = Level.FINER;
  setupDefaultLogHandler();
//  (new Logger('clean_sync')).level = Level.FINEST;
//  (new Logger('clean_ajax')).level = Level.FINE;
  run();
}

run() {

  MongoConnection mongoConnection;
  LockRequestor lockRequestor;
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

  DataReference updateLock;
  TransactorClient transactorClient;
  TransactorServer transactorServer;
  Subscriber subscriber;

group('collection_modification',() {
  setUp((){
    var mongoUrl = "mongodb://0.0.0.0/mongoProviderTest";
    var host = "127.0.0.1";
    var lockerPort = 27002;
    var msPort = 27001;
    Cache cache = new Cache(new Duration(milliseconds: 10), 10000);
    return LockRequestor.connect(host, lockerPort)
      .then((LockRequestor _lockRequestor) => lockRequestor = _lockRequestor)
      .then((_) => mongoConnection = new MongoConnection(mongoUrl, lockRequestor, cache: cache))
      .then((_) => mongoConnection.init())
      .then((_) => transactorServer = new TransactorServer(mongoConnection))
      .then((_) => transactorServer.init())
      .catchError((e,s) => print("$e, $s")).then((_){
        transactorServer.registerBeforeCallback('addAll', allowOperation);
        transactorServer.registerBeforeCallback('change', allowOperation);
        transactorServer.registerBeforeCallback('removeAll', allowOperation);

        pub = new Publisher();
        MultiRequestHandler requestHandler = new MultiRequestHandler();
        requestHandler.registerHandler('sync',pub.handleSyncRequest);
        requestHandler.registerHandler('sync-operation', transactorServer.handleSyncRequest);
        connection = createLoopBackConnection(requestHandler);
        updateLock = new DataReference(false);
        subscriber = new Subscriber.config(connection, new IdGenerator(), defaultSubscriptionFactory, defaultTransactorFactory,
            updateLock);
      })
      .then((_) => subscriber.init("prefix"))
      .then((_) => mongoConnection.transact((MongoDatabase mdb) => mdb.dropCollection('random')))
      .then((_){
          pub.publish('a', (_) {
            return mongoConnection.collection("random").find({});
          });

          pub.publish('b', (_) {
            return mongoConnection.collection("random").find({'a': 'hello'});
          });

          pub.publish('c', (_) {
            return mongoConnection.collection("random").find({'a.a': 'hello'});
          });

          pub.publish('withArgs', (args) {
            return mongoConnection.collection("random").find({'a': args['name']});
          });

          pub.publish('mapped_pos', (_) {
            return mongoConnection.collection("random").find({'b': 3}).fields(['a']);
          });

          pub.publish('mapped_neg', (_) {
            return mongoConnection.collection("random").find({'b': 3}).excludeFields(['a']);
          });


          subAll = subscriber.subscribe('a', 'random')..restart();
          colAll = subAll.collection;
          subAll2 = subscriber.subscribe('a', 'random')..restart();
          colAll2 = subAll2.collection;
          subA = subscriber.subscribe('b', 'random')..restart();
          colA = subA.collection;
          subAa = subscriber.subscribe('c', 'random')..restart();
          colAa = subAa.collection;
          subArgs = subscriber.subscribe('withArgs', 'random')..restart(args: {'name': 'aa'});
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
      return item.dispose().catchError((e,s) => print("$e, $s"));
    }).then((_) => new Future.delayed(new Duration(milliseconds: 200)))
      .then((_) => transactorServer.close())
      .then((_) => lockRequestor.close())
      .then((_) => mongoConnection.close());
    });

  executeSubscriptionActions(List actions) {
    return mongoConnection.transact((MongoDatabase mdb) => mdb.dropCollection('random'))
    .catchError((e, s){
      print('cannot drop collection, ignoring the error');
    })
    .then((_) {
      return subAll.initialSync;}).then((_) {
      return subAll2.initialSync;}).then((_) =>
    subA.initialSync).then((_) =>
    subAa.initialSync).then((_) =>
    subArgs.initialSync).then((_) =>
    Future.forEach(actions, (action) {
      try {
      action();
      } catch (e) { print(e); }
      return new Future.delayed(new Duration(milliseconds: 200));
    }));
  }

  skip_test('data added to the set is not cloned, if it is already DataMap', () {
    DataMap data = new DataMap.from({'_id': '0', 'a' : 'aa'});

    List actions = [
      () => colAll.add(data),
      () => expect(data == colAll.first, isTrue),
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
      () => colAll2.first['colAll'] = 'brave new world',
      () => expect(stripIds(colAll), unorderedEquals([
        {'colAll' : 'brave new world'}
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
    checkPreventUpdate(Subscription subscription){
      return (event) => expect(subscription.updateLock.value, isTrue);
    }
    colAll2.onChangeSync.listen(checkPreventUpdate(subAll2));
    colA.onChangeSync.listen(checkPreventUpdate(subA));
    colAa.onChangeSync.listen(checkPreventUpdate(subAa));
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
      () => expect(colA, unorderedEquals([{'_id' : '2', 'a': 'hello',
         '__clean_collection': 'random'}])),
      () => expect(colAa, unorderedEquals([])),
      () => dataA['a'] = {'a': 'hello'},
      () => expect(colAa, unorderedEquals([{'_id' : '2', 'a' : {'a': 'hello'},
         '__clean_collection': 'random'}])),
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
    Subscription subMapped = subscriber.subscribe('mapped_pos', 'random')..restart();
    DataSet colMapped = subMapped.collection;

    List actions = [
      () => colAll.add({'a': 1, 'b': 3, 'c': 2}),
      () => colAll.add({'a': 2, 'b': 4, 'c': 2}),
      () => expect(stripIds(colMapped), equals([{'a': 1}])),
      () => newSub = subscriber.subscribe(subMapped.resourceName,
               subMapped.mongoCollectionName)..restart(),
      () => expect(colMapped, unorderedEquals(newSub.collection)),
      () {subMapped.dispose(); newSub.dispose();}
    ];

    return executeSubscriptionActions(actions);

  });

  test('test collection excluded fields', () {
    Subscription newSub;
    Subscription subMapped = subscriber.subscribe('mapped_neg', 'random')..restart();
    DataSet colMapped = subMapped.collection;

    List actions = [
      () => colAll.add({'a': 1, 'b': 3, 'c': 2}),
      () => colAll.add({'a': 2, 'b': 4, 'c': 2}),
      () => expect(stripIds(colMapped), equals([{'b': 3, 'c': 2}])),
      () => newSub = subscriber.subscribe(subMapped.resourceName,
               subMapped.mongoCollectionName)..restart(),
      () => expect(colMapped, unorderedEquals(newSub.collection)),
      () {subMapped.dispose(); newSub.dispose();}
    ];

    return executeSubscriptionActions(actions);

  });

  test('test subscription restart & dispose', () {

      var sub = subscriber.subscribe('a', 'random')..restart();
      sub.initialSync.then((_){
        sub.collection.add({'price':'value'});
      });

      new Future.delayed(new Duration(milliseconds: 200), () {
        sub.restart();
        sub.dispose();
      });

      return new Future.delayed(new Duration(milliseconds: 1000), () {});

  });

  test('test data list manipulation', () {
    DataMap morders = new DataMap();
    DataList orders = new DataList();
    colAll2.onChangeSync.listen((event){
      expect(subAll2.updateLock.value, isTrue);
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
      () {colAll.add(data1); colAll.removeBy('_id', data1['_id']); colAll.add(data1);},
      () => expect(colAll, unorderedEquals([data1])),
      () {colAll.removeBy('_id', data1['_id']);},
      () {colAll.add(data1);},
      () => expect(colAll, unorderedEquals([data1])),
    ];

    return executeSubscriptionActions(actions);

  });


  test("restart immediately renews initialSync", (){
    return subArgs.initialSync.then((_){
      Future oldinitialSync = subArgs.initialSync;
      subArgs.restart(args: null);
      expect(subArgs.initialSync == oldinitialSync, isFalse);
      return subArgs.initialSync;
    });
  });

  test('restart', () {
    List actions = [
      // close unneeded subscriptions to have nicer log
      (){
        subAll2.dispose();
        subA.dispose();
        subAa.dispose();
      },
      () {colAll.addAll([{'a': 'aa'}, {'a': 'bb'}]);},
      () => expect(stripIds(colArgs), unorderedEquals([{'a': 'aa'}])),
      () => subArgs.restart(args: {'name': 'bb'}),
      () => expect(stripIds(colArgs), unorderedEquals([{'a': 'bb'}])),
      () => colAll.add({'a': 'bb'}),
      () => colAll.add({'a': 'aa'}),
      () => expect(stripIds(colArgs), unorderedEquals([{'a': 'bb'}, {'a': 'bb'}])),
    ];
    return executeSubscriptionActions(actions);
  });

  test('changes immediately between restart are still saved correctly', () {
    List actions = [
      () {colArgs.add({'a': 'aa'}); subArgs.restart(args: {'name': 'bb'});},
      () => expect(stripIds(colAll), unorderedEquals([{'a': 'aa'}])),
      () => expect(colArgs.isEmpty, isTrue),
    ];
    return executeSubscriptionActions(actions);
  });

  test('new data are present immediately after initial_sync completes', () {
    List actions = [
      () => colAll.addAll([{'a': 'aa'}, {'a': 'bb'}]),
      () {
        subArgs.restart(args: {'name': 'bb'});
        subArgs.initialSync.then((_){
          expect(stripIds(colArgs), unorderedEquals([{'a': 'bb'}]));
        });
      },
    ];
    return executeSubscriptionActions(actions);
  });
});
}
