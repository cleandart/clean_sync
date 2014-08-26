
library mongo_server_test;

import 'package:unittest/unittest.dart';
import 'dart:async';
import 'package:clean_sync/mongo_server.dart';
import 'package:clean_sync/server.dart';
import 'package:clean_sync/mongo_client.dart';
import 'package:logging/logging.dart';
import 'package:useful/useful.dart';
import 'package:clean_data/clean_data.dart';
import 'package:clean_sync/operations.dart';
import 'dart:convert';

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

allowOperation(ServerOperationCall) => true;


void main() {
  setupDefaultLogHandler();
  logger.level = Level.WARNING;

  run();
}

void run() {
  group("Mongo Server", () {

    MongoServer server;
    MongoClient client;
    IdGenerator idgen = new IdGenerator();
    String testCollectionUser = 'testCollectionUser';
    String lastOperation;
    String lastBeforeMsg;
    DataReference lastOperationRef1 = new DataReference("");
    DataReference lastOperationRef2 = new DataReference("");

    setUp(() {
      lastOperation = "";
      server = new MongoServer(27001, "mongodb://0.0.0.0/mongoServerTest");
      return server.start()
          .then((_) => server.db.dropCollection(testCollectionUser))
          .then((_) => server.db.removeLocks())
          .then((_) {

        client = new MongoClient("127.0.0.1", 27001);

        server.registerOperation("save",
            operation: (ServerOperationCall opCall){
              return opCall.colls[0].add(opCall.args, "");
            }
        );
        server.registerOperation("delete",
            operation: (ServerOperationCall opCall) {
              return opCall.colls[0].remove(opCall.args["_id"],"");
            }
        );
        server.registerOperation("set",
            before: (ServerOperationCall opCall) {
              if (opCall.args.containsKey("_id")) throw new ValidationException("Cannot set _id of document");
              if ((opCall.docs is List) && (opCall.docs.length > 1)) throw new ValidationException("Too many documents");
              return null;
            },
            operation: (ServerOperationCall opCall) {
              opCall.args.forEach((k,v) => opCall.docs[0][k] = v);
            }
        );
        server.registerOperation("throw",
            before: (ServerOperationCall opCall) {
              lastOperation = "before";
              if (opCall.args['throw'] == 'before') throw new ValidationException("Before threw");
              return null;
            },
            operation: (ServerOperationCall opCall) {
              lastOperation = "operation";
              if (opCall.args['throw'] == 'operation') throw new Exception("Operation threw");
            },
            after: (ServerOperationCall opCall) {
              lastOperation = "after";
              if (opCall.args['throw'] == 'after') throw new Exception("After threw");
            }
        );

        server.registerOperation("change ref1",
            before: (ServerOperationCall opCall) {
              lastOperationRef1.value = "before";
              return null;
            },
            operation: (ServerOperationCall opCall) {
              lastOperationRef1.value = "operation";
            },
            after: (ServerOperationCall opCall) {
              lastOperationRef1.value = "after";
            }
        );

        server.registerOperation("change ref2",
            before: (ServerOperationCall opCall) {
              lastOperationRef2.value = "before";
              return null;
            },
            operation: (ServerOperationCall opCall) {
              lastOperationRef2.value = "operation";
            },
            after: (ServerOperationCall opCall) {
              lastOperationRef2.value = "after";
            }
        );
        server.registerOperation("dummy", operation: (opCall){});

        server.registerOperation("test returns",
            operation: (ServerOperationCall opCall) {
              lastOperation = "operation";
            }
        );

        server.registerBeforeCallback('addAll', allowOperation);
        server.registerBeforeCallback('change', allowOperation);
        server.registerBeforeCallback('removeAll', allowOperation);
        server.registerBeforeCallback('dummy', allowOperation);
        server.registerBeforeCallback('save', allowOperation);
        server.registerBeforeCallback('throw', allowOperation);
        server.registerBeforeCallback('change ref1', allowOperation);
        server.registerBeforeCallback('change ref2', allowOperation);
        server.registerBeforeCallback('set', allowOperation);

        return client.connected;
      });
    });

    tearDown(() {
      return Future.wait([server.db.collection(testCollectionUser).collection.drop(),
          server.db.collection(historyCollectionName(testCollectionUser)).collection.drop(),
          client.close()])
          .then((_) => server.close());
    });

    test("save document", () {
      var id = idgen.next();
      var args = {'_id' : '$id', 'name' : 'sample', 'credit' : 5000};
      return client.connected.then((_) =>
        client.performOperation('save', colls: [testCollectionUser], args: args)
      )
      .catchError((e,s) => logger.shout("error", e, s))
      .then((_) {
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
          expect(client.performOperation("set", args: data, docs: [['$id',data[COLLECTION_NAME]]]), completes);
        });
      });

    });

    test("should not perform operation if before throws", () {
      var caught = false;
      return client.connected.then((_) {
        return client.performOperation("throw", args:{'throw':'before'})
            .then((res){
            print(res);
            expect(res, contains('error'));
        });
      });
    });

    test("should not perform after if operation throws", () {
      return client.connected.then((_) {
        return client.performOperation("throw", args:{'throw':'operation'})
        .then((res){
          expect(res, contains('error'));
          expect(lastOperation, equals("operation"));
        });
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

    test("should handle many operations sent right after each other (3000)", () {
      List<Future> ops = [];
      return client.connected.then((_) {
        for (int i = 0; i < 3000; i++) {
          if (i % 10 == 0) logger.fine("At $i");
          ops.add(client.performOperation("dummy", args: {"i":'$i'}));
        }
        return expect(Future.wait(ops), completes);
      });
    });

    test("should report error if there was no entry found", () {
      var caught = false;
      return client.connected.then((_) {
        var operation = client.performOperation("set", docs: [['1',testCollectionUser]], args:{'x':'y'})
        .then((e) {
          expect(e,isMap);
          expect(e["error"], isMap);
          expect(e["error"].containsKey('doc_not_found'), isTrue);
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

    test("should stop executing before callbacks after explicit result", () {
      lastOperation = "";
      lastBeforeMsg = "";
      List callbacks = [
        (ServerOperationCall opCall) { lastBeforeMsg = "1"; return null;},
        (ServerOperationCall opCall) { lastBeforeMsg = "2"; return null;},
        (ServerOperationCall opCall) { lastBeforeMsg = "3"; return true;},
        (ServerOperationCall opCall) { lastBeforeMsg = "4"; return null;},
      ];
      callbacks.forEach((c) => server.registerBeforeCallback("test returns", c));

      return client.connected.then((_) => client.performOperation("test returns"))
      .then((_) {
          expect(lastOperation, equals("operation"));
          expect(lastBeforeMsg, equals("3"));
      });
    });

    test("should not perform operation if false is returned from callbacks", () {
      lastOperation = "";
      lastBeforeMsg = "";
      List callbacks = [
        (ServerOperationCall opCall) { lastBeforeMsg = "1"; return null;},
        (ServerOperationCall opCall) { lastBeforeMsg = "2"; return false;},
        (ServerOperationCall opCall) { lastBeforeMsg = "3"; return true;},
        (ServerOperationCall opCall) { lastBeforeMsg = "4"; return null;},
      ];
      callbacks.forEach((c) => server.registerBeforeCallback("test returns", c));

      return client.connected.then((_) => client.performOperation("test returns"))
      .then((_) {
          expect(lastOperation, equals(""));
          expect(lastBeforeMsg, equals("2"));
      });
    });

    test("should not perform operation if no explicit result was returned", () {
      lastOperation = "";
      lastBeforeMsg = "";
      List callbacks = [
        (ServerOperationCall opCall) { lastBeforeMsg = "1"; return null;},
        (ServerOperationCall opCall) { lastBeforeMsg = "2"; return null;},
        (ServerOperationCall opCall) { lastBeforeMsg = "3"; return null;},
        (ServerOperationCall opCall) { lastBeforeMsg = "4"; return null;},
      ];
      callbacks.forEach((c) => server.registerBeforeCallback("test returns", c));

      return client.connected.then((_) => client.performOperation("test returns"))
      .then((_) {
          expect(lastOperation, equals(""));
          expect(lastBeforeMsg, equals("4"));
      });
    });

    test("should decode message with accents correctly", () {
      List<Future> ops = [];
      return client.connected.then((_) {
        for (int i = 0; i < 2000; i++) {
          if (i % 10 == 0) logger.fine("At $i");
          ops.add(client.performOperation("dummy", args: {"message":'Příšerná ťarcha'}));
        }
        return expect(Future.wait(ops), completes);
      });
    });
  });
}