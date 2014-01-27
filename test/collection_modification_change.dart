library collection_modification_change;

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

class BareConnectionMock extends Mock implements Connection {}
class IdGeneratorMock extends Mock implements IdGenerator {}

main() {

  var config = new SimpleConfiguration();
  config.timeout = null;
  unittestConfiguration = config;

  MongoDatabase mongodb;
  DataSet colAll;
  DataSet colAll2;
  DataSet colA;
  DataSet colAa;
  Connection connection;
  Subscription subAll;
  Subscription subAll2;
  Subscription subA;
  Subscription subAa;

  DataMap data1;
  DataMap data2;
  DataMap data3;
  DataMap data4;

  Publisher pub;


  setUp((){
    mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest');

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

        data1 = new DataMap.from({'_id': '0', 'colAll' : 'added from colAll'});
        data2 = new DataMap.from({'_id': '1', 'colAll2': 'added from colAll2'});
        data3 = new DataMap.from({'_id': '2', 'a': 'hello'});
        data4 = new DataMap.from({'a' : 'hello'});
    });
  });
  
  tearDown(() {
    List itemsToClose = [
      subAll,
      subAll2,
      subA,
      subAa
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
      () => colAll2.add(data2),
      () => expect(colAll, unorderedEquals([data1, data2])),
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

  test('test collection filtered add', () {
    List actions = [
      () => colAll.add(data1),
      () => expect(colA.isEmpty, isTrue),
      () => colAll.add(data3),
      () => expect(colA, unorderedEquals([data3])),
    ];
    
    return executeSubscriptionActions(actions);
    
  });

  test('test collection filtered change', () {
    List actions = [
      () => colAll.add(data3),
      () => colA.first['a'] = data4,
      () => expect(colAa, unorderedEquals([
        {'_id' : '2', 'a' : data4}
      ])),
      () => colAa.first['a'] = 'hello',
      () => expect(colA, unorderedEquals([data3])),
    ];
    
    return executeSubscriptionActions(actions);
    
  });

  test('test collection filtered remove', () {
    List actions = [
      () => colA.add(data3),
      () => colAll.removeBy('_id', '2'),
      () => expect(colA.isEmpty, isTrue),
      () => expect(colAll.isEmpty, isTrue),
    ];
    
    return executeSubscriptionActions(actions);
    
  });

}
