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

class BareConnectionMock extends Mock implements Connection {}
class IdGeneratorMock extends Mock implements IdGenerator {}

main() {

  unittestConfiguration.timeout=null;

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

        pub.publish('mapped', (_) {
          return mongodb.collection("random").find({});
        }, project: (Map elem){
          elem.remove('a');
          elem['aa'] = 'it works gr8';
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
        subMapped = new Subscription('mapped', connection, 'author5', new IdGenerator('e'), {});
        colMapped = subMapped.collection;

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
      () {colAll2.add(data2); colAll.add(data3);},
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

  test('test collection mapped', () {
    List actions = [
      () => colAll.add({'a': 1, 'b': 2}),
      () => expect(colMapped, equals([{'b': 2, '_id': 'a-1', 'aa': 'it works gr8'}])),
    ];

    return executeSubscriptionActions(actions);

  });


  solo_test('big data performance', () {
    print('tu');
    var data = new DataMap();
    for(int i=0; i<2000; i++) {
      print('init $i');
      data['$i'] = {'key' : i};
    }
    num i=-1;
    return
      mongodb.dropCollection('random').then((_) =>
      mongodb.removeLocks()).then((_) =>
      subAll.initialSync).then((_) =>
      subAll2.initialSync).then((_) =>
      colAll.add(data)).then((_) =>
      Future.forEach(new List.filled(10000, null), (_) {
      print(++i);
      print(data);
      data['${i%1000}']['key']='changed $i';
      return new Future.delayed(new Duration(milliseconds: 500));
    }).then((_){
      return new Future.delayed(new Duration(seconds: 5));
    }).then((_){
      expect(colAll, unorderedEquals(colAll2));
    }));

  });


}
