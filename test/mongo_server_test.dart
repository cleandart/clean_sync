
library client_test;

import 'package:unittest/unittest.dart';
import 'dart:async';
import 'package:clean_sync/mongo_server.dart';
import 'package:clean_sync/server.dart';
import 'package:clean_sync/mongo_client.dart';
import 'package:logging/logging.dart';
import 'package:useful/useful.dart';
import 'package:clean_data/clean_data.dart';

Logger logger = new Logger('mongo_wrapper_logger');
class IdGenerator {
  var current;
  IdGenerator() {
    current = new DateTime.now().millisecondsSinceEpoch;
  }

  next() {
    return ++current;
  }
}

void main() {
  setupDefaultLogHandler();
  logger.level = Level.FINER;
  run();
}

void run() {
  group("Mongo Server", () {

    MongoServer server;
    MongoClient client;
    IdGenerator idgen = new IdGenerator();
    String testCollectionUser = 'testCollectionUser';
    String lastOperation;
    DataReference lastOperationRef1 = new DataReference("");
    DataReference lastOperationRef2 = new DataReference("");

    setUp(() {
      lastOperation = "";
      server = new MongoServer(27001, "mongodb://0.0.0.0/mongoServerTest");
      return server.start().then((_) {

        client = new MongoClient("127.0.0.1", 27001);

        server.registerOperation("save",
            operation: (fullDocs, args, MongoProvider collection){
              return collection.add(args, "");
            }
        );
        server.registerOperation("delete",
            operation: (fullDocs, args, MongoProvider collection) {
              return collection.remove(args["_id"],"");
            }
        );
        server.registerOperation("set",
            before: (fullDocs, args, user, MongoProvider collection) {
              if (args.containsKey("_id")) throw new ValidationException("Cannot set _id of document");
              if ((fullDocs is List) && (fullDocs.length > 1)) throw new ValidationException("Too many documents");

            },
            operation: (fullDocs, args, MongoProvider collection) {
              if (fullDocs is List) fullDocs = fullDocs[0];
              args.forEach((k,v) => fullDocs[k] = v);
              return server.db.collection(fullDocs[COLLECTION_NAME]).change(fullDocs['_id'], fullDocs, "");
            }
        );
        server.registerOperation("throw",
            before: (fullDocs, args, user, MongoProvider collection) {
              lastOperation = "before";
              if (args['throw'] == 'before') throw new ValidationException("Before threw");
            },
            operation: (fullDocs, args, MongoProvider collection) {
              lastOperation = "operation";
              if (args['throw'] == 'operation') throw new Exception("Operation threw");
            },
            after: (fullDocs, args, user, MongoProvider collection) {
              lastOperation = "after";
              if (args['throw'] == 'after') throw new Exception("After threw");
            }
        );

        server.registerOperation("change ref1",
            before: (fullDocs, args, user, MongoProvider collection) {
              lastOperationRef1.value = "before";
            },
            operation: (fullDocs, args, MongoProvider collection) {
              lastOperationRef1.value = "operation";
            },
            after: (fullDocs, args, user, MongoProvider collection) {
              lastOperationRef1.value = "after";
            }
        );

        server.registerOperation("change ref2",
            before: (fullDocs, args, user, MongoProvider collection) {
              lastOperationRef2.value = "before";
            },
            operation: (fullDocs, args, MongoProvider collection) {
              lastOperationRef2.value = "operation";
            },
            after: (fullDocs, args, user, MongoProvider collection) {
              lastOperationRef2.value = "after";
            }
        );
        server.registerOperation("dummy");

      });
    });

    tearDown(() {
      return server.db.collection(testCollectionUser).collection.drop().then((_) => server.close());
    });

    test("save document", () {
      var id = idgen.next();
      var args = {'_id' : '$id', 'name' : 'sample', 'credit' : 5000};
      var future = client.connected.then((_){
        return client.performOperation('save', collections: testCollectionUser, args: args);
      }).catchError((e,s) => print('Error: $e, $s'));

      return future.then((_) {
        expect(server.db.collection(testCollectionUser).find(args).findOne(), completes);
      });
    });

    test("collection name should be included in document", () {
      // given
      var id = idgen.next();
      var data = {'_id' : '$id', 'name' : 'some name', 'credit' : 5000};
      return server.db.collection(testCollectionUser).add(data, "").then((_) {
        return server.db.collection(testCollectionUser).find(data).findOne();
      }).then((data){
        return client.connected.then((_){
          data['name'] = 'another name';
          data.remove('_id');
          //then
          expect(client.performOperation("set", docs: [['$id',data[COLLECTION_NAME]]], args: data), completes);
        });
      });

    });

    test("should not perform operation if before throws", () {
      var caught = false;
      return client.connected.then((_) {
        var operation = client.performOperation("throw", args:{'throw':'before'})
        .catchError((e,s) {
          expect(e,isMap);
          expect(e.containsKey('validation'), isTrue);
          caught = true;
        }).then((_){
          expect(caught, isTrue);
        });
      });
    });

    test("should not perform after if operation throws", () {
      return client.connected.then((_) {
        var operation = client.performOperation("throw", args:{'throw':'operation'})
        .whenComplete((){
          expect(lastOperation, equals("operation"));
        });
        expect(operation, throws);
      });
    });

    test("should handle operations sent right after each other", () {
      return client.connected.then((_) {
        expect(Future.wait([
          client.performOperation("dummy", args: {'a':'1'}),
          client.performOperation("dummy", args: {'a':'2'}),
          client.performOperation("dummy", args: {'a':'3'}),
          client.performOperation("dummy", args: {'a':'4'}),
          client.performOperation("dummy", args: {'a':'5'}),
          client.performOperation("dummy", args: {'a':'6'}),
          client.performOperation("dummy", args: {'a':'7'}),
        ]),completes);
      });
    });

    test("should report error if there was no entry found", () {
      var caught = false;
      return client.connected.then((_) {
        var operation = client.performOperation("set", docs: [['1',testCollectionUser]], args:{'x':'y'})
        .catchError((e,s) {
          expect(e,isMap);
          expect(e.containsKey('query'), isTrue);
          caught = true;
        }).then((_) => expect(caught, isTrue));
      });
    });

    test("\'before\' of next function should execute after \'after\' of the previous", () {
      bool failureOccured = false;
      lastOperationRef2.onChangeSync.listen((_) {
        if (lastOperationRef1.value != "after") failureOccured = true;
      });
      lastOperationRef1.onChangeSync.listen((_) {
        if (lastOperationRef2.value != "") failureOccured = true;
      });
      return client.connected.then((_) {
        var future1 = client.performOperation("change ref1");
        var future2 = client.performOperation("change ref2");
        return Future.wait([future1, future2]);
      }).then((_) => new Future(() => expect(failureOccured, isFalse)));
    });

    test("functions should execute in order: before, operation, after", () {
      bool failureOccured = false;
      String previousOperation = "";
      lastOperationRef1.onChangeSync.listen((_) {
        if ((previousOperation == "") && (lastOperationRef1.value != "before")) failureOccured = true;
        if ((previousOperation == "before") && (lastOperationRef1.value != "operation")) failureOccured = true;
        if ((previousOperation == "operation") && (lastOperationRef1.value != "after")) failureOccured = true;
        previousOperation = lastOperationRef1.value;
      });
      return client.connected.then((_) {
        return client.performOperation("change ref1");
      }).then((_) => new Future(() => expect(failureOccured, isFalse)));
    });

    test("operations should work with \'unpacked\' list for one doc only", () {
      var id = idgen.next();
      var data = {'_id' : '$id', 'name' : 'some name', 'credit' : 5000};
      return server.db.collection(testCollectionUser).add(data, "")
        .then((_) => client.connected.then((_){
             data['name'] = 'another name';
             data.remove('_id');
             expect(client.performOperation("set", docs: ['$id',testCollectionUser], args: data), completes);
        }));
    });

  });
}