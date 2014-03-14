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

main(){
  unittestConfiguration.timeout = null;
  setupDefaultLogHandler();
  run();
}

run() {

  MongoDatabase mongodb;
  DataSet col;
  DataSet colAll;
  Connection connection;
  Subscription sub;
  Subscription subAll;
  Publisher pub;

  executeSubscriptionActions(List actions) {
    return
    mongodb.dropCollection('random').then((_) =>
    mongodb.removeLocks()).then((_) =>
    subAll.initialSync).then((_) =>
    sub.initialSync).then((_) =>
    Future.forEach(actions, (action) {
      action();
      return new Future.delayed(new Duration(milliseconds: 200));
    }));
  }

  setUp((){
    mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest');

    return Future.wait(mongodb.init)
    .then((_) => mongodb.dropCollection('random'))
    .then((_) => mongodb.removeLocks()).then((_){

        pub = new Publisher();
        pub.publish('a', (args) {
          return mongodb.collection("random").find({'name': args['name']});
        });

        MultiRequestHandler requestHandler = new MultiRequestHandler();
        requestHandler.registerDefaultHandler(pub.handleSyncRequest);
        connection = createLoopBackConnection(requestHandler);

        sub = new Subscription('a', connection, 'author1', new IdGenerator('a'),
            {'name': 'andy'});
        subAll = new Subscription('a', connection, 'author1', new IdGenerator('a'),
            {'name': 'andy'});
        col = sub.collection;
        colAll = sub.collection;
    });
  });

  tearDown(() {
    List itemsToClose = [
      sub,
    ];

    return Future.forEach(itemsToClose, (item) {
      return item.close();
    }).then((_) => mongodb.close());
  });


  test("restart immediately renews initialSync", (){
    return sub.initialSync.then((_){
      Future oldinitialSync = sub.initialSync;
      sub.restart(null).then((_){});
      expect(sub.initialSync == oldinitialSync, isFalse);
    });
  });

  test('test collection add', () {
    List actions = [
      () => colAll.addAll([{'name': 'andy', 'data': 'a'},
                           {'name': 'andy', 'data': 'b'},
                           {'name': 'sid', 'data': 'c'},
                           {'name': 'sid', 'data': 'd'},
                     ]),


      colAll.add(data1),
      () => expect(colAll2, unorderedEquals([data1])),
      () {colAll2.add(data2); colAll.add(dataA);},
      () => expect(colAll, unorderedEquals(colAll2)),
    ];

    return executeSubscriptionActions(actions);

  });

  test("restart with different args", (){
    return sub.initialSync.then((_){


      Future oldinitialSync = sub.initialSync;
      sub.restart(null).then((_){});
      expect(sub.initialSync == oldinitialSync, isFalse);
    });
  });



}
