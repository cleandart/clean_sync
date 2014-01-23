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
import 'package:clean_ajax/common.dart';
import 'package:clean_data/clean_data.dart';
import 'package:logging/logging.dart';
import './mongo_provider_test.dart';


Random rng = new Random();

const PROB_REMOVE = 0.1;
const PROB_ADD = 0.3;

prob(p) {
  return p > rng.nextDouble();
}


class BareConnectionMock extends Mock implements Connection {}
class IdGeneratorMock extends Mock implements IdGenerator {}

main() {
  DataSet currCollection;
  DataSet wholeCollection;
  MongoDatabase mongodb;
  DataSet sender;
  DataSet receiver;
  DataSet receiverb;
  DataSet receiverc;
  Connection connection;
  Subscription subSender;
  Subscription subReceiver;
  Subscription subReceiverb;
  Subscription subReceiverc;

  Logger log = new Logger('');
  log.level = Level.WARNING;

  Logger.root.onRecord.listen((LogRecord rec) {
    print('${rec.level.name}: ${rec.time}: ${rec.message}');
  });


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

        var _idGenerator = new IdGeneratorMock();
        subSender = new Subscription('a', connection, 'author1', _idGenerator, {});
        sender = subSender.collection;

        subReceiver = new Subscription('a', connection, 'author2', _idGenerator, {});
        receiver = subReceiver.collection;
        subReceiverb = new Subscription('b', connection, 'author3', _idGenerator, {});
        subReceiverc = new Subscription('c', connection, 'author4', _idGenerator, {});

        receiverb = subReceiverb.collection;
        receiverc = subReceiverc.collection;


    });
  });

  randomChoice(Iterable iter) {
    var list = new List.from(iter);
    return list[rng.nextInt(list.length)];
  }

  var allValues=['hello', 'world', 1];
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

  makeRandomChange(DataSet coll, Set ids) {
    String id = (rng.nextInt(9)+1).toString();
    if (prob(PROB_ADD)) {
      // add
      if (ids.contains(id)) {
        return false;
      } else {
        print('before add $id');
        print(coll);
        ids.add(id);
        coll.add(new DataMap.from({'_id': id}), author: null);
        print('after add $id');

        return true;
      }
    }
    else if (prob(PROB_REMOVE)) {
      // remove
      if (!ids.contains(id)) return false;
      else {
        print('before remo $id');
        print(coll);
        coll.removeWhere((d)=>d['_id']==id, author: null);
        ids.remove(id);
        print('before remo $id');
        return true;
      }
    } else {
      // change
      if(ids.contains(id)){
        print('before change $id');

        print(coll);
        var data = coll.firstWhere((d) => d['_id'] == id);
        var a = coll.length;
        randomChangeMap(data);
        print(data);
        var b = coll.length;
        assert(a==b);
        print('after change $id');

        return true;
      } else {
        return false;
      }
    }
  }


  test('test random', () {

  var ids = new Set();

  var action = (){
    Subscription toChangeSub = randomChoice([subSender, subReceiver]);
    print(toChangeSub);
    return makeRandomChange(toChangeSub.collection, ids);
  };
//  var action = () => makeRandomChange(sender, ids);
  var makeExpects = () {
    expect(stripPrivateFieldsList(receiver),
           unorderedEquals(stripPrivateFieldsList(sender)));
//    expect(stripPrivateFieldsList(sender.where((d)=>d['a']=='hello')),
//           unorderedEquals(stripPrivateFieldsList(receiverb)));
//    expect(stripPrivateFieldsList(
//        sender.where((d) => (d['a'] is Map && d['a']['a'] == 'hello'))),
//        unorderedEquals(stripPrivateFieldsList(receiverc)));
  };

    var times=[20, 50, 100, 200, 400, 800, 1600, 3200];
//    var times=[300, 400, 800, 1600, 3200];
    return subSender.initialSync.then((_) =>
    subReceiver.initialSync).then((_) =>
    subReceiverb.initialSync).then((_) =>
    subReceiverc.initialSync).then((_) =>

    Future.forEach(new List.filled(1000, null), (_) {
        do{} while(!action());
//        sender.where((d) => (d.containsKey('a') && d['a'] is Map && d['a']['a'] == 'hello'));
//        print(receiverc);
        bool end = false;
        return Future.forEach(times, (time){
          if(end){
            return new Future.value(0);
          } else
          return new Future.delayed(new Duration(milliseconds: time), (){
            try{
              makeExpects();
              end = true;
            } catch(e,s){
              if(time == times.last){
                print(s);
                throw e;
              }
            }
          });
        });
    }));

  });

  test('test subs', () {

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

    List actions = [
      () => sender.add(data, author: null),
      () => print(receiver),
      () => expect(stripPrivateFields(receiver.first), equals(stripPrivateFields(data))),
      () {print('assign!!!!!'); receiver.first['b'] = 'bbb'; sender.first['b'] = 'bb';},
      () => print(sender),
      () => print(receiver),
      () => expect(stripPrivateFieldsList(sender), equals(stripPrivateFieldsList(receiver))),
    ];



    return
    mongodb.dropCollection('random').then((_) =>
    mongodb.removeLocks()).then((_) =>
    subSender.initialSync).then((_) =>
    subReceiver.initialSync).then((_) =>
    subReceiverb.initialSync).then((_) =>
    subReceiverc.initialSync).then((_) =>
    Future.forEach(actions, (action) {
      action();
      return new Future.delayed(new Duration(milliseconds: 200));
    }));

  });
}


