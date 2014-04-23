library limit_and_sort_test;

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
  DataSet colRandomSorted;

  Connection connection;
  LoopBackTransport transport;
  Subscription subRandom;
  Subscription subRandomSorted;

  Map dataA = {'name' : 'a', 'age' : 46};
  Map dataB = {'name' : 'b', 'age' : 57};
  Map dataC = {'name' : 'c', 'age' : 68};
  Map dataD = {'name' : 'd', 'age' : 23};
  Map dataE = {'name' : 'e', 'age' : 72};
  Map dataF = {'name' : 'f', 'age' : 90};
  Map dataG = {'name' : 'g', 'age' : 102};
  
  DataMap a = new DataMap.from(dataA);
  DataMap b = new DataMap.from(dataB);
  DataMap c = new DataMap.from(dataC);
  DataMap d = new DataMap.from(dataD);
  DataMap e = new DataMap.from(dataE);
  DataMap f = new DataMap.from(dataF);
  DataMap g = new DataMap.from(dataG);
  
  Publisher pub;

  setUp((){
    mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest');

    return Future.wait(mongodb.init)
    .then((_) => mongodb.dropCollection('random'))
    .then((_) => mongodb.removeLocks()).then((_){

        pub = new Publisher();

        pub.publish('random', (_) {
          return mongodb.collection("random");
        });
        
        pub.publish('randomSorted', (_) {
          return mongodb.collection("random").sort({"age": ASC}).limit(3);
        });

        MultiRequestHandler requestHandler = new MultiRequestHandler();
        requestHandler.registerDefaultHandler(pub.handleSyncRequest);
        transport = new LoopBackTransport(requestHandler.handleLoopBackRequest);
        connection = new Connection.config(transport);

        subRandom = new Subscription('random', connection, 'author_random', new IdGenerator('random'));
        colRandom = subRandom.collection;

        subRandomSorted = new Subscription('randomSorted', connection, 'author_random', new IdGenerator('randomSorted'));
        colRandomSorted = subRandomSorted.collection;
        
        subRandom.restart();
        subRandomSorted.restart();
        
        return Future.wait([subRandom.initialSync, subRandomSorted.initialSync]);
    });
  });

  tearDown(() {
    List itemsToClose = [subRandom, subRandomSorted];

    return Future.forEach(itemsToClose, (item) => item.dispose())
      .then((_) => new Future.delayed(new Duration(milliseconds: 500)))
      .then((_) => mongodb.close());
  });

  void printClientRandom() {
    print("");
    print("Client data (all):");
    colRandom.forEach((data) {
      print(data);
    });
    print("");
  };
  
  void printClientRandomSorted() {
    print("");
    print("Client data (sorted):");
    colRandomSorted.forEach((data) {
      print(data);
    });
    print("");
  };
  
  Future printServerRandomSorted() {
    return mongodb.collection("random").sort({"age": ASC}).limit(3).data().then((Map data) {
      print("");
      print("Server data:");
      data["data"].forEach((data) => print(data));
      print("");
    });
  }
  
  test("test limit and sort", () {
    Completer fullSyncFinished = new Completer();
    
    StreamSubscription fullSyncSub = subRandom.onFullSync.listen((_) {
      logger.finer("Full sync finished");
      fullSyncFinished.complete();
    });
    
    colRandom.add(a);
    colRandom.add(b);
    colRandom.add(c);
//    a["name"] = "aa";
//    a["name"] = "aaa";
//    b["age"] = 44;
//    colRandom.remove(c);
    
    return fullSyncFinished.future
      .then((_) => new Future.delayed(new Duration(milliseconds: 100), () {
        return printServerRandomSorted().then((_) {
          printClientRandom();
          printClientRandomSorted();
          fullSyncFinished = new Completer();
          
          colRandom.remove(a);
          colRandom.remove(b);
          colRandom.add(e);
          colRandom.add(f);
          colRandom.add(g);
          c["age"] = 65;
          
          return fullSyncFinished.future;
        });
      }))
      .then((_) => new Future.delayed(new Duration(milliseconds: 100), () {
        return printServerRandomSorted().then((_) {
          printClientRandom();
          printClientRandomSorted();
          fullSyncFinished = new Completer();
          
          colRandom.add(d);
          
          return fullSyncFinished.future;
        });
      }))
      .then((_) => new Future.delayed(new Duration(milliseconds: 100), () {
        return printServerRandomSorted().then((_) {
          printClientRandom();
          printClientRandomSorted();
        });
      }));
  });
}
