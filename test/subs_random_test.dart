library subs_random_test;

import "package:unittest/unittest.dart";
import "package:mock/mock.dart";
import "package:clean_sync/server.dart";
import "dart:async";
import 'dart:math';
import './mongo_provider_test.dart';
import 'package:clean_sync/client.dart';
import 'package:clean_ajax/client.dart';
import 'package:clean_ajax/client_backend.dart';
import 'package:clean_ajax/server.dart';
import 'package:clean_data/clean_data.dart';
import 'package:logging/logging.dart';
import 'package:useful/useful.dart';
import 'package:clean_sync/transactor_server.dart';
import 'package:clean_sync/id_generator.dart';
import 'package:clean_lock/lock_requestor.dart';


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

allowOperation(ServerOperationCall) => true;

class BareConnectionMock extends Mock implements Connection {}
class IdGeneratorMock extends Mock implements IdGenerator {}

Logger testLogger = new Logger('clean_sync.subs_random_test');

main() {
  var config = new SimpleConfiguration();
  config.timeout = null;
  unittestConfiguration = config;
  hierarchicalLoggingEnabled = true;
//  (new Logger('clean_ajax')).level = Level.INFO;
  testLogger.level = Level.FINER;
  (new Logger('clean_ajax')).level = Level.FINE;
//  (new Logger('clean_sync')).level = Level.FINER;


  setupDefaultLogHandler();
  run(1000000, new Cache(new Duration(milliseconds: 100), 10000), failProb: 0);
//  run(1000000, new DummyCache(), failProb: 0.05);
}

run(count, cache, {failProb: 0}) {
  DataSet currCollection;
  DataSet wholeCollection;
  MongoConnection mongoConnection;
  LockRequestor lockRequestor;
  DataSet colAll;
  DataSet colAll2;
  DataSet colA;
  DataSet colAa;
  Connection connection;
  LoopBackTransportStub transport;
  Subscription subAll;
  Subscription subAll2;
  Subscription subA;
  Subscription subAa;
  Subscription subNoMatch;
  Subscriber subscriber;
  TransactorServer transactorServer;

  Publisher pub;
  DataReference updateLock;
  ftransactorByAuthor(author) => new TransactorClient(connection, updateLock,
      author, new IdGenerator('f'));

group('subs_random_test', () {
  setUp((){
    var mongoUrl = "mongodb://0.0.0.0/mongoProviderTest";
    var url = "127.0.0.1";
    var port = 27001;
    var lockerPort = 27002;
    updateLock = new DataReference(false);
    return LockRequestor.connect(url, lockerPort)
    .then((LockRequestor _lockRequestor) => lockRequestor = _lockRequestor)
    .then((_) => mongoConnection = new MongoConnection(mongoUrl, lockRequestor))
    .then((_) => mongoConnection.init())
    .then((_) => transactorServer = new TransactorServer(mongoConnection))
    .then((_) => transactorServer.init())
    .then((_) => mongoConnection.transact((MongoDatabase mdb) => mdb.dropCollection('random')))
    .then((_) {
        transactorServer.registerBeforeCallback('addAll', allowOperation);
        transactorServer.registerBeforeCallback('change', allowOperation);
        transactorServer.registerBeforeCallback('removeAll', allowOperation);

        pub = new Publisher();
        var versionProvider = mongoConnection.collection("random");
        pub.publish('a', (_) {
          return mongoConnection.collection("random").find({});
        });

        pub.publish('b', (_) {
          return mongoConnection.collection("random").find({'a': 'hello'});
        });

        pub.publish('c', (_) {
          return mongoConnection.collection("random").find({'a.a': 'hello'});
        });

        pub.publish('d', (_) {
          return mongoConnection.collection("random").find({'noMatch': 'noMatch'});
        });


        MultiRequestHandler requestHandler = new MultiRequestHandler();
        requestHandler.registerDefaultHandler(pub.handleSyncRequest);
        requestHandler.registerHandler('sync-operation', transactorServer.handleSyncRequest);
        transport = new LoopBackTransportStub(
            requestHandler.handleLoopBackRequest, null);
        connection = new Connection.config(transport);

        subscriber = new Subscriber.config(connection, new IdGenerator(), defaultSubscriptionFactory, defaultTransactorFactory,
            updateLock);
        subscriber.init('prefix');

        subAll = subscriber.subscribe('a', 'random')..restart();
        colAll = subAll.collection;
        subAll2 = subscriber.subscribe('a', 'random')..restart();
        colAll2 = subAll2.collection;
        subA = subscriber.subscribe('b','random')..restart();
        colA = subA.collection;
        subAa = subscriber.subscribe('c', 'random')..restart();
        colAa = subAa.collection;
        subNoMatch = subscriber.subscribe('d', 'random')..restart();

    });
  });

  tearDown(() {
    return Future.wait([transactorServer.close(), lockRequestor.close(), mongoConnection.close()]);
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
        testLogger.finer('before add \n $coll');
        if (probMap == 1 || prob(probMap)) {
          coll.add({});
        } else
        if (prob(probElem)){
          coll.add(randomChoice(allValues));
        } else {
          coll.add([]);
        }
        testLogger.finer('after add');
        return true;
    } else
    if(!prob(probChange)){
      // remove
        if (coll.length == 0) return false;
        testLogger.finer('before remove \n $coll');
        coll.remove(randomChoice(coll));
        testLogger.finer('after remove');
        return true;
    }
    else {
      // change
      if (coll.length == 0) return false;
      var index = rng.nextInt(coll.length);
      var data = new List.from(coll)[index];

      testLogger.finer('before change \n $coll');
      if (data is Map) {
        randomChangeMap(data);
      } else
      if (data is List) {
        randomChangeCollection(data, topLevel: false);
      } else {
        coll[index] = randomChoice(allValues);
      }
      testLogger.finer('after change: $data');
      return true;
    }
  };

  randomChangeCollection = _randomChangeCollection;


  test('test random subscription modification', () {

  var action = (){
    for (int i=0; i<rng.nextInt(10); i++) {
      Subscription toChangeSub = randomChoice([subAll, subAll2]);
      testLogger.finer('collection to change: ${toChangeSub}');
      randomChangeCollection(toChangeSub.collection);
    }
    if (prob(failProb)){
      transport.fail(1, new Duration(seconds: 1));
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
      expect(subNoMatch.version, equals(subAll.version));

    });
    if (checkGetData) {
      for (Subscription sub in [subAll]) {
        Subscription newSub;
        res = res
          .then((_) =>
            newSub = new Subscription(sub.resourceName, 'random', connection,
                new IdGeneratorMock(), ftransactorByAuthor('dummyAuthor'), updateLock)..restart())
          .then((_) =>
            newSub.initialSync)
          .then((_) =>
            newSub.dispose()
          ).then((_) {
            expect(newSub.collection, unorderedEquals(sub.collection));
          });
      }
    }
    return res;
  };

    var times=[0, 30, 40, 50, 100, 200, 400, 800, 1600, 3200, 6400];
    var i=0;

    var watch = new Stopwatch()..start();
    var watchTime = 0;
    var watchElems = 0;
    mongoConnection.transact((MongoDatabase mdb) => mdb.createCollection('random'));

    new Timer.periodic(new Duration(seconds: 60), (_){
      var bound = [subAll.version, subAll2.version, subA.version, subAa.version, subNoMatch.version].reduce(min);
      mongoConnection.collection('random').deleteHistory(bound);
    });

    return
    subAll.initialSync.then((_) =>
    subAll2.initialSync).then((_) =>
    subA.initialSync).then((_) =>
    subAa.initialSync).then((_) =>
    Future.forEach(new List.filled(count, null), (_) {
        i++;
        var val = watch.elapsedMilliseconds;
        watch.reset();
        watchTime = watchTime*0.99 + val;
        watchElems = watchElems*0.99 + 1;
        var watchAverage = watchTime / watchElems;

        testLogger.info('$i (${watchAverage.round()} ms per modif)');
        action();
        testLogger.info('$colAll');
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
});
}
