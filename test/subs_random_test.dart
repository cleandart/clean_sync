library subs_random_test;

import "package:unittest/unittest.dart";
import "package:clean_sync/server.dart";
import "dart:async";
import 'dart:math';
import './mongo_provider_test.dart';
import 'package:clean_sync/client.dart';
import 'package:unittest/mock.dart';
import 'package:clean_ajax/client.dart';
import 'package:clean_ajax/client_backend.dart';
import 'package:clean_ajax/server.dart';
import 'package:clean_data/clean_data.dart';
import 'package:logging/logging.dart';


Random rng = new Random();

// affect how map is modified
const PROB_REMOVE = 0.1;
const PROB_ADD = 0.3;

// affect how collection is modified
const PROB_CHANGE = 0.95;

prob(p) {
  return p > rng.nextDouble();
}

final Logger logger = new Logger('clean_sync');


class BareConnectionMock extends Mock implements Connection {}
class IdGeneratorMock extends Mock implements IdGenerator {}

main() {

  var config = new SimpleConfiguration();
  config.timeout = null;
  unittestConfiguration = config;



  hierarchicalLoggingEnabled = true;
  logger.level = Level.WARNING;
  Logger.root.onRecord.listen((LogRecord rec) {
    print('${rec.message}');
  });


  DataSet currCollection;
  DataSet wholeCollection;
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

  Publisher pub;

  mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest');

  setUp((){
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
    });
  });

  randomChoice(Iterable iter) {
    var list = new List.from(iter);
    return list[rng.nextInt(list.length)];
  }

  var allValues=['hello', 'world', 1];
  var allKeys=['a','b','c'];


  var randomChangeCollection = null;

  randomChangeMap(Map data) {
    var key = randomChoice(allKeys);
    if (data.containsKey(key)) {
      if (data[key] is Map) {
        randomChangeMap(data[key]);
      } else if (data[key] is DataList){
        randomChangeCollection(data[key], allowList: true);
      } else {
        data[key] = randomChoice(allValues);
      }
    } else {
      data[key] = randomChoice(allValues);
    }

    if (data[key] is! Map && data[key] is! List && prob(PROB_ADD)) {
      if(prob(0.5)){
        data[key] = new DataList();
        randomChangeCollection(data[key], allowList:true);
      } else {
        data[key] = new DataMap();
        randomChangeMap(data[key]);
      }
    }

    if (prob(PROB_REMOVE)) {
      data.remove(key);
    }
  }

  randomChangeCollection = (dynamic coll, {allowList: false}) {
    var probAdd = (){
      if(coll.length<5) return 1;
      if(coll.length>15)return 0;
      return rng.nextDouble();
    };


      if (!prob(coll.length/10)) {
        // add
          logger.finer('before add \n $coll');
          if (prob(0.5) || !allowList) {
            coll.add(new DataMap.from({}));
          } else {
            coll.add([]);
          }
          logger.finer('after add');
          return true;
      } else
      if(!prob(PROB_CHANGE)){
        // remove
          if (coll.length == 0) return false;
          logger.finer('before remo \n $coll');
          coll.remove(randomChoice(coll));
          logger.finer('before remo');
          return true;
      }
    else {
      // change
      if (coll.length == 0) return false;
      var data = randomChoice(coll);
      logger.finer('before change \n $coll');
      if (data is Map) {
        randomChangeMap(data);
      } else {
        randomChangeCollection(data);
      }
      logger.finer('after change: $data');
      return true;
    }
  };


  test('test random', () {

  var action = (){
    for (int i=0; i<5; i++) {
      Subscription toChangeSub = randomChoice(
          [subAll, subAll2]);
      randomChangeCollection(toChangeSub.collection);
    }
  };

  mongoEquals(dynamic obj, List<String> what, pattern, {allowList: true}){
    if (what.isEmpty) {
      return obj == pattern;
    }
    var key = what.first;
    var rest = what.sublist(1);
    if (obj is Map) {
      return (obj.containsKey(what.first)) && mongoEquals(obj[key], what.sublist(1), pattern);
    }
    if (obj is List && allowList) {
      return obj.any((e) => mongoEquals(e, what, pattern, allowList: false));
    }
    return false;
  }

  Future makeExpects({checkGetData: true}) {
    Future res = new Future.sync((){
      expect(stripPrivateFieldsList(colAll2),
             unorderedEquals(stripPrivateFieldsList(colAll)));
      expect(stripPrivateFieldsList(colAll.where((d) => mongoEquals(d, ['a'], 'hello'))),
          unorderedEquals(stripPrivateFieldsList(colA)));
      expect(stripPrivateFieldsList(
          colAll.where((d) => mongoEquals(d, ['a', 'a'], 'hello'))),
          unorderedEquals(stripPrivateFieldsList(colAa)));
    });
    if (checkGetData) {
      for (Subscription sub in [subAll]) {
        Subscription newSub;
        res = res
          .then((_) =>
            newSub = new Subscription(sub.collectionName, connection, 'dummyAuthor', new IdGeneratorMock()))
          .then((_) =>
              newSub.initialSync)
          .then((_){
            return newSub.close();
          }).then((_) {
            expect(stripPrivateFieldsList(newSub.collection), unorderedEquals(stripPrivateFieldsList(sub.collection)));
          });
      }
    }
    return res;
  };

    var times=[50, 100, 200, 400, 800, 1600, 3200, 6400, 10000];
    var i=0;

    var watch = new Stopwatch()..start();
    var watchTime = 0;
    var watchElems = 0;
    mongodb.create_collection('random');

    new Timer.periodic(new Duration(seconds: 60), (_){
      mongodb.collection('random').deleteHistory(
          [subAll.version, subAll2.version, subA.version, subAa.version].reduce(min));
    });

    return
    Future.wait(mongodb.init).then((_) =>
    subAll.initialSync).then((_) =>
    subAll2.initialSync).then((_) =>
    subA.initialSync).then((_) =>
    subAa.initialSync).then((_) =>
    Future.forEach(new List.filled(100000, null), (_) {
        i++;
        var val = watch.elapsedMilliseconds;
        watch.reset();
        watchTime = watchTime*0.99 + val;
        watchElems = watchElems*0.99 + 1;
        var watchAverage = watchTime / watchElems;

        print('$i (${watchAverage.round()} ms per modif)');
        action();

//        print(receiver);
        bool end = false;
        return Future.forEach(times, (time){
          bool checkGetData = prob(0.1);
          if(end){
            return new Future.value(0);
          } else
          return new Future.delayed(new Duration(milliseconds: time), () =>
              makeExpects(checkGetData: checkGetData)).then((_){
                end = true;
              }).catchError((e){
                if(time == times.last){
                  print('author1 $colAll');
                  print('author2 $colAll2');
                  print('author2 $colA');
                  print('author4 $colAa');
                  throw e;
                }
              });
        });
    }));

  });

  skip_test('test subs', () {

    DataMap data = new DataMap.from({'_id': '0'});
    DataMap data1 = new DataMap.from({'_id': '1', 'b': 'bbb'});
    DataMap data2 = new DataMap.from({'_id': '2', 'c': 'ccc'});


//    List actions = [
//      () => sender.add(data, author: null),
//      () => expect(stripPrivateFields(receiver.first), equals(data)),
//      () => expect(receiverb, isEmpty),
//      () => data['a'] = 'hello',
//      () => expect(stripPrivateFields(receiverb.first), equals(data)),
//      () => data.remove('a'),
//      () => expect(receiverb, isEmpty),
//      () => data['a'] = 1,
//      () => expect(receiverc, isEmpty),
//      () => data['a'] = new DataMap.from({}),
//      () => expect(receiverc, isEmpty),
//      () => data['a']['a'] = 'hello',
//      () => expect(stripPrivateFields(receiverc.first), equals(data)),
//    ];

//    List actions = [
//      () => sender.add(data, author: null),
//      () => print(receiver),
//      () => expect(stripPrivateFields(receiver.first), equals(stripPrivateFields(data))),
//      () {print('assign!!!!!'); receiver.first['b'] = 'bbb'; sender.first['b'] = 'bb';},
//      () => print(sender),
//      () => print(receiver),
//      () => expect(stripPrivateFieldsList(sender), equals(stripPrivateFieldsList(receiver))),
//    ];


    List actions = [
      () => print(colAll2),
      () => mongodb.collection('random').add({'_id': '0', 'a': 10}, 'ja'),
      () => print(colAll2),
      () => mongodb.collection('random').deprecatedChange('0', {'b': 20}, 'ja'),
      () => expect(stripPrivateFieldsList(colAll2), unorderedEquals([{'_id': '0', 'a': 10, 'b': 20}])),
      () => mongodb.collection('random').deprecatedChange('0', {'a': {'a': 10}}, 'ja'),
      () => expect(stripPrivateFieldsList(colAll2), unorderedEquals([{'_id': '0', 'a': {'a': 10}, 'b': 20}])),

      () => print(colAll2),
    ];




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

  });
}


