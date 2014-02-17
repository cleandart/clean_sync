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
const PROB_TOP_CHANGE = 0.95;
const PROB_NESTED_CHANGE = 0.5;

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
  Logger.root.level = Level.WARNING;
  Logger.root.onRecord.listen((LogRecord rec) {
    print('${rec.loggerName} ${rec.message} ${rec.error} ${rec.stackTrace}');
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
  Subscription subNoMatch;

  DataMap data1;
  DataMap data2;
  DataMap data3;
  DataMap data4;

  Publisher pub;

  mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest');

  setUp((){
    cacheFactory() => new Cache(new Duration(milliseconds: 10), 10000);
    return Future.wait(mongodb.init)
    .then((_) => mongodb.dropCollection('random'))
    .then((_) => mongodb.removeLocks()).then((_){
        pub = new Publisher();
        var versionProvider = mongodb.collection("random");
//        var versionProvider = null;
        pub.publish('a', (_) {
          return mongodb.collection("random").find({});
        }, cacheFactory: cacheFactory);

        pub.publish('b', (_) {
          return mongodb.collection("random").find({'a': 'hello'});
        }, cacheFactory: cacheFactory);

        pub.publish('c', (_) {
          return mongodb.collection("random").find({'a.a': 'hello'});
        }, cacheFactory: cacheFactory);

        pub.publish('d', (_) {
          return mongodb.collection("random").find({'noMatch': 'noMatch'});
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
        subNoMatch = new Subscription('d', connection, 'author5',
            new IdGenerator('e'), {});

        data1 = new DataMap.from({'_id': '0', 'colAll' : 'added from colAll'});
        data2 = new DataMap.from({'_id': '1', 'colAll2': 'added from colAll2'});
        data3 = new DataMap.from({'_id': '2', 'a': 'hello'});
        data4 = new DataMap.from({'a' : 'hello'});
    });
  });

  randomChoice(Iterable iter) {
    var list;
    if (iter is List) {
      list = iter;
    } else {
      list = iter.toList();
    }
    return list[rng.nextInt(list.length)];
  }

  var allValues=['hello', 'world', 1, null];
  var allKeys=['a','b','c'];


  var randomChangeCollection = null;

  randomChangeMap(Map data) {
    var key = randomChoice(allKeys);
    if (data.containsKey(key)) {
      if (data[key] is Map) {
        randomChangeMap(data[key]);
      } else if (data[key] is DataList){
        randomChangeCollection(data[key], topLevel: false);
      } else {
        data[key] = randomChoice(allValues);
      }
    } else {
      data[key] = randomChoice(allValues);
    }

    if (data[key] is! Map && data[key] is! List && prob(PROB_ADD)) {
      if(prob(0.9)){
        data[key] = new DataList();
        randomChangeCollection(data[key], topLevel: false);
      } else {
        data[key] = new DataMap();
        randomChangeMap(data[key]);
      }
    }

    if (prob(PROB_REMOVE)) {
      data.remove(key);
    }
  }

  _randomChangeCollection(var coll, {topLevel: true}) {
    var probMap = topLevel?1:0.5;
    var probChange = topLevel?PROB_TOP_CHANGE:PROB_NESTED_CHANGE;
    var maxLength = topLevel?10:2;
    var probElem = 0.3;

    if (!prob(coll.length/maxLength)) {
      // add
        logger.finer('before add \n $coll');
        if (probMap == 1 || prob(probMap)) {
          coll.add({});
        } else
        if (prob(probElem)){
          coll.add(randomChoice(allValues));
        } else {
          coll.add([]);
        }
        logger.finer('after add');
        return true;
    } else
    if(!prob(probChange)){
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
      var index = rng.nextInt(coll.length);
      var data = new List.from(coll)[index];

      logger.finer('before change \n $coll');
      if (data is Map) {
        randomChangeMap(data);
      } else
      if (data is List) {
        randomChangeCollection(data, topLevel: false);
      } else {
        coll[index] = randomChoice(allValues);
      }
      logger.finer('after change: $data');
      return true;
    }
  };

  randomChangeCollection = _randomChangeCollection;

  test('test random', () {

  var action = (){
    for (int i=0; i<rng.nextInt(10); i++) {
      Subscription toChangeSub = randomChoice(
          [subAll, subAll2]);
      randomChangeCollection(toChangeSub.collection);
    }
  };

  mongoEquals(dynamic obj, List<String> what, pattern, {allowList: true}){
    if (what.isEmpty) {
      if (obj is List && allowList) {
        return obj.any((e) => mongoEquals(e, what, pattern, allowList: false));
      } else {
        return obj == pattern;
      }
    }
    var key = what.first;
    var rest = what.sublist(1);
    if (obj is Map) {
      return (obj.containsKey(what.first)) && mongoEquals(obj[key], rest, pattern);
    }
    if (obj is List && allowList) {
      return obj.any((e) => mongoEquals(e, what, pattern, allowList: false));
    }
    return false;
  }



  Future makeExpects({checkGetData: true}) {
    Future res = new Future.sync((){
      expect(colAll2, unorderedEquals(colAll));
      expect(colAll.where((d) => mongoEquals(d, ['a'], 'hello')),
          unorderedEquals(colA));
      expect(colAll.where((d) => mongoEquals(d, ['a', 'a'], 'hello')),
          unorderedEquals(colAa));
      expect(subNoMatch.version == subAll.version, isTrue);

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
            expect(newSub.collection, unorderedEquals(sub.collection));
          });
      }
    }
    return res;
  };

    var times=[30, 40, 50, 100, 200, 400, 800, 1600, 3200, 6400, 10000];
    var i=0;

    var watch = new Stopwatch()..start();
    var watchTime = 0;
    var watchElems = 0;
    mongodb.create_collection('random');

    new Timer.periodic(new Duration(seconds: 60), (_){
      var bound = [subAll.version, subAll2.version, subA.version, subAa.version, subNoMatch.version].reduce(min);
      mongodb.collection('random').deleteHistory(bound);
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

        print(colAll);
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
}
