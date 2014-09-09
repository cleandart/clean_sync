library mongo_provider_random_test;

import "package:unittest/unittest.dart";
import "package:clean_sync/server.dart";
import "dart:async";
import 'dart:math';
import './mongo_provider_test.dart';
import 'package:useful/useful.dart';
import 'package:logging/logging.dart';
import 'package:clean_sync/mongo_server.dart';
import 'package:clean_lock/lock_requestor.dart';


Random rng = new Random();

const PROB_REMOVE = 0.05;
const PROB_ADD = 0.3;

prob(p) {
  return p > rng.nextDouble();
}


Logger testLogger = new Logger('clean_sync.subs_random_test');

main() {
  var config = new SimpleConfiguration();
  config.timeout = null;
  unittestConfiguration = config;
  hierarchicalLoggingEnabled = true;
  testLogger.level = Level.INFO;
  setupDefaultLogHandler();
  run(1000);
}

run(count) {
  MongoProvider currCollection;
  MongoProvider wholeCollection;
  MongoDatabase mongodb;
  MongoServer mongoServer;

  setup(selector) {
    var mongoUrl = 'mongodb://127.0.0.1/mongoProviderTest';
    var host = "127.0.0.1";
    var msPort = 27001;
    var lockerPort = 27002;
    return LockRequestor.connect(host, lockerPort)
      .then((LockRequestor lockRequestor) => mongodb = new MongoDatabase(mongoUrl, lockRequestor))
      .then((_) => mongoServer = new MongoServer(27001, mongodb))
      .then((_) => mongoServer.start())
      .then((_) => mongodb.dropCollection('random'))
      .then((_){
        wholeCollection = mongodb.collection('random');
        currCollection = selector(mongodb.collection('random'));
      });
  }

    randomChoice(Iterable iter) {
      var list = new List.from(iter);
      return list[rng.nextInt(list.length)];
    }

    var allValues=['hello', 'world', 1, null];
    var allKeys=['a','b','c'];

    randomChangeMap(Map data) {
      var key = randomChoice(allKeys);
      if (data.containsKey(key)) {
        if (data[key] is Map) {
          randomChangeMap(data[key]);
        } else {
          data[key] = randomChoice(allValues);
        }
      } else {
        data[key] = randomChoice(allValues);
      }

      if (data[key] is! Map && prob(PROB_ADD)) {
        data[key] = new Map();
        randomChangeMap(data[key]);
      }

      if (prob(PROB_REMOVE)) {
        data.remove(key);
      }
    }

    Future makeRandomChange(MongoProvider coll, Set ids) {
      String id = rng.nextInt(4).toString();
      if (prob(PROB_ADD)) {
        // add
        if (ids.contains(id)) {
          return new Future.value();
        } else {
          ids.add(id);
          return coll.add({'_id': id}, '');
        }
      }
      else if (prob(PROB_REMOVE)) {
        // remove
        if (!ids.contains(id)) {
          return new Future.value();
        } else {
          ids.remove(id);
          return coll.remove(id, '');
        }
      } else {
        // change
        if (!ids.contains(id)) {
          return new Future.value();
        } else {
          return coll.find({'_id':id}).data().then((datas) {
            var _data = datas['data'][0];
            randomChangeMap(_data);
            return coll.change(id, _data, '');
          });
        }
      }
    }

    toStringOrdered(List<Map> data) {
      compare(a, b) {
        return a['_id'].compareTo(b['_id']);
      }
      mapToStringOrdered(Map m, StringBuffer sb) {
        sb.write('{');
        List toWrite = new List.from(m.keys)..sort();
        for (var key in toWrite) {
          sb.write('${key}: ');
          if(m[key] is! Map) {
            sb.write(m[key].toString());
          } else {
            mapToStringOrdered(m[key], sb);
          }
          if (key != toWrite.last) sb.write(', ');
        }
        sb.write('}');
      }

      data.sort(compare);
      StringBuffer sb = new StringBuffer('[');
      for(var d in data){
        mapToStringOrdered(d, sb);
        sb.write(', ');
      }
      sb.write(']');
      return sb.toString();
    }

    _teardown() {
      mongoServer.close();
    };

   _test(){
    // given
    List dataStart;
    List dataEnd;
    Set ids;
    ids = new Set();
    var lastVersion = 0;

    return Future.forEach(new List.filled(count, 0) , (_) {
      return currCollection.data().then(
        (data) {
          dataStart = data['data'];
          return Future.forEach(new List.filled(20, 0), (_) =>
              makeRandomChange(wholeCollection, ids));

        }).then((_) => currCollection.data())
          .then((data){ dataEnd = data['data']; })
          .then((_) => currCollection.diffFromVersion(lastVersion))
          .then((currCollection) {
            currCollection['diff'].forEach((e) => lastVersion = max(lastVersion, e['version']));
            handleDiff(currCollection['diff'], dataStart);
            testLogger.info('${toStringOrdered(dataEnd)}');
            expect(toStringOrdered(dataStart), equals(toStringOrdered(dataEnd)));
          });
    });
  };

  runTest(selector) {
    return setup(selector)
      .then((_) => _test())
      .then((_) => _teardown());
  }

  test('Make a lot of changes and see whether getData and getDiff behave consistently.', () {
    List modifiers = [
//      (MongoProvider m) => m.find({'b': {'\$gt' : {}}}).sort({'a': 1}).limit(3),
//      (MongoProvider m) => m.find({'b': {'\$gt' : {}}}).sort({'a': -1}).limit(3),
      (MongoProvider m) => m,
      (MongoProvider m) => m.find({'a.a': {'\$gt': {}}}),
      (MongoProvider m) => m.find({'a.a.a': {'\$gt': {}}}),
      (MongoProvider m) => m.find({'b.b': 'hello'}),
      (MongoProvider m) => m.find({'a': {'\$gt': {}}}),
    ];

    return Future.forEach(modifiers, (modifier) => runTest(modifier));
  });
}
