
library client_test;

import 'package:unittest/unittest.dart';
import 'dart:async';
import 'package:clean_sync/mongo_server.dart';
import 'package:clean_sync/server.dart';
import 'package:clean_sync/mongo_client.dart';
import 'package:logging/logging.dart';
import 'package:useful/useful.dart';

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
              if (args.containsKey("_id")) throw new Exception("Cannot set _id of document");
              if ((fullDocs is List) && (fullDocs.length > 1)) throw new Exception("Too many documents");

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
        server.registerOperation("dummy");

      });
    });

    tearDown(() {
      server.db.collection(testCollectionUser).collection.drop();

      return server.close();
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
        print(data);
        return client.connected.then((_){
          data['name'] = 'another name';
          data.remove('_id');
          //then
          expect(client.performOperation("set", docs: [['$id',data[COLLECTION_NAME]]], args: data), completes);
        });
      });

    });

    test("should not perform operation if before throws", () {
      return client.connected.then((_) {
        var operation = client.performOperation("throw", args:{'throw':'before'})
        .whenComplete((){
          expect(lastOperation, equals("before"));
        });
        expect(operation, throws);
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

    solo_test("should handle operations sent right after each other", () {
      return client.connected.then((_) {
        expect(Future.wait([
           client.performOperation("dummy", args: {'a':'1'}),
           client.performOperation("dummy", args: {'a':'2'})
        ]),completes);
      });
    });

  });
}