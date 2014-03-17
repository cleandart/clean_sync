library connection_recovery_test;

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

Logger logger = new Logger('clean_sync');

main(){
  hierarchicalLoggingEnabled = true;
  unittestConfiguration.timeout = null;
  logger.level = Level.FINER;
  setupDefaultLogHandler();
  run();
}

run() {

  MongoDatabase mongodb;
  DataSet colRandom;

  Connection connection;
  LoopBackTransportStub transport;
  Subscription subRandom;

  Map dataA;
  Map dataB;
  Map dataC;

  Publisher pub;

  setUp((){
    mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest');

    return Future.wait(mongodb.init)
    .then((_) => mongodb.dropCollection('random'))
    .then((_) => mongodb.removeLocks()).then((_){

        pub = new Publisher();
        
        pub.publish('random', (_) {
          return mongodb.collection("random").find({});
        });

        MultiRequestHandler requestHandler = new MultiRequestHandler();
        requestHandler.registerDefaultHandler(pub.handleSyncRequest);
        transport = new LoopBackTransportStub(requestHandler.handleLoopBackRequest);
        connection = new Connection.config(transport);

        subRandom = new Subscription('random', connection, 'author_random', new IdGenerator('random'));
        colRandom = subRandom.collection;
        
        dataA = {'name' : 'a', 'age' : 46};
        dataB = {'name' : 'b', 'age' : 57};
        dataC = {'name' : 'c', 'age' : 68};
    });
  });

  tearDown(() {
    List itemsToClose = [subRandom];

    return Future.forEach(itemsToClose, (item) => item.dispose())
      .then((_) => new Future.delayed(new Duration(milliseconds: 500)))
      .then((_) => mongodb.close());
  });

  executeSubscriptionActions(List actions) {
    return mongodb.dropCollection('random').then((_) =>
      mongodb.removeLocks()).then((_) =>
      subRandom.initialSync).then((_) =>
      Future.forEach(actions, (action) {
        action();
        return new Future.delayed(new Duration(milliseconds: 200));
      }));
  }

  test("test subscription's connection recovery", () {
    
    connection.onDisconnected.listen((_) {
      logger.finer("Connection down");
    });
    
    connection.onConnected.listen((_) {
      logger.finer("Connection restored");
    });
    
    return executeSubscriptionActions([]).then((_) {
      return Future.forEach(new List.filled(1000, 0) , (_) {
        Completer resyncFinished = new Completer();
        Completer fullSyncFinished = new Completer();
        
        StreamSubscription resyncSub = subRandom.onResyncFinished.listen((_) {
          logger.finer("Resync finished");
          resyncFinished.complete();
        });
        
        StreamSubscription fullSyncSub = subRandom.onFullSync.listen((_) {
          logger.finer("Full sync finished");
          fullSyncFinished.complete();
        });
        
        DataMap a = new DataMap.from(dataA);
        DataMap b = new DataMap.from(dataB);
        DataMap c = new DataMap.from(dataC);
        
        transport.fail(0.6, new Duration(milliseconds: 500));
        colRandom.add(a);
        colRandom.add(b);
        colRandom.add(c);
        a["name"] = "aa";
        a["name"] = "aaa";
        b["age"] = 44;
        colRandom.remove(c);

        return resyncFinished.future
          .then((_) => resyncSub.cancel())
          .then((_) => fullSyncFinished.future)
          .then((_) => fullSyncSub.cancel())
          .then((_) {
            return Future.wait([
              mongodb.collection('random').find().data().then((Map data) {
                logger.finer("${data["data"].length}");
                return expect(data["data"], unorderedEquals(colRandom));
              }),
            ]);
          });
      });
    });
  });
}
