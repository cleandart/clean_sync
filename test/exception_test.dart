library exception_test;

import "package:unittest/unittest.dart";
import "package:clean_sync/profiling.dart";
import 'package:clean_sync/client.dart';
import 'package:clean_sync/id_generator.dart';
import 'package:mock/mock.dart';
import 'package:clean_ajax/client.dart';
import 'package:clean_ajax/client_backend.dart';
import 'package:clean_ajax/server.dart';
import 'package:clean_data/clean_data.dart';
import 'package:useful/useful.dart';
import 'dart:async';


class IdGeneratorMock extends Mock implements IdGenerator {}
class TransactorMock extends Mock implements Transactor {}

main(){
  setupDefaultLogHandler();
  run();
}

run() {
  group('group ', () {
    MongoDatabase mongodb;
    Connection connection;
    Publisher pub;
    Subscription sub;
    Transactor transactor;
    DataReference updateLock;

    setUp((){
      updateLock = new DataReference(false);
      mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest');
      transactor = new Transactor(connection, updateLock, 'author', new IdGeneratorMock());
      return Future.wait(mongodb.init)
        .then((_) => mongodb.dropCollection('random'))
        .then((_) => mongodb.removeLocks()).then((_){
          pub = new Publisher();
          pub.publish('a', (_) {
            throw new ArgumentError('__TEST__: Error');
          });

          MultiRequestHandler requestHandler = new MultiRequestHandler();
          requestHandler.registerDefaultHandler(pub.handleSyncRequest);
          connection = createLoopBackConnection(requestHandler);
      });
    });

    tearDown(() {
      return sub.dispose().then((_) => mongodb.close());
    });

    skip_test('Exception in initial sync is caught on client-side', () {
      var _idGenerator = new IdGeneratorMock();
      var callback = expectAsync((_){});
      sub = new Subscription('a', 'random', connection, _idGenerator, transactor, updateLock)..restart();
      return sub.initialSync.then((_){}, onError: callback);
    });

    skip_test('Exception in beforeRequest is caught on client-side', () {
      var newdata = new DataMap.from({"_id": "id"});
      var testvalue = expectAsync((value) {
        expect(value, equals(newdata));
      });

      var beforeRequest = (value, args) {
        testvalue(value);
        throw new ArgumentError("__TEST__ : No!");
      };

      pub.publish('b', (_) {
        return mongodb.collection("random");
      }, beforeRequest: beforeRequest);
      var _idGenerator = new IdGeneratorMock();
      var callback = expectAsync((_){});

      sub = new Subscription('b', 'random', connection, _idGenerator, transactor, updateLock)..restart();
      sub.initialSync.then((_) {
        sub.errorStream.listen(callback);
        sub.collection.add(newdata);
      });
    });
  });
}
