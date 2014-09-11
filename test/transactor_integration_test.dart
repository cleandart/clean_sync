library transactor_integration_test;
import 'package:clean_sync/mongo_server.dart';
import 'package:clean_sync/mongo_client.dart';
import 'package:clean_sync/client.dart';
import 'package:clean_sync/server.dart';
import 'package:unittest/unittest.dart';
import 'package:clean_data/clean_data.dart';
import 'package:clean_ajax/server.dart';
import 'package:clean_ajax/client.dart';
import 'package:clean_sync/operations.dart';
import 'package:clean_sync/id_generator.dart';
import 'package:mock/mock.dart';
import 'dart:async';
import 'package:logging/logging.dart';
import 'package:clean_lock/lock_requestor.dart';

class SubscriptionMock extends Mock implements Subscription {
  var mongoCollectionName;
  var collection;
}

allowOperation(ServerOperationCall) => true;

main() {
  run();
}

run() {
  Transactor transactor;
  MongoClient mongoClient;
  MongoServer mongoServer;
  MongoConnection mongoConnection;
  LockRequestor lockRequestor;
  DataReference updateLock;
  Connection connection;

  String collectionName = "transactorTest";
  String logCollectionName = "transactorTestLog";


  List operations = [
      [
        new ServerOperation('send money',
          before: (ServerOperationCall opCall) {
            if (!opCall.docs[0].containsKey("credit") || !opCall.docs[1].containsKey("credit"))
              throw new ValidationException("Documents don't contain credit field");
            if (opCall.docs[0]["credit"] < opCall.args["amount"])
              throw new ValidationException("First document does not have enough credit");
            return true;
          },
          operation: (ServerOperationCall opCall) {
            opCall.docs[0]["credit"] -= opCall.args["amount"];
            opCall.docs[1]["credit"] += opCall.args["amount"];
            return opCall.colls[0].add({"_id":"8", "log":"player bought"}, "");
          }),
        new ClientOperation('send money',
          operation: (ClientOperationCall opCall) {
            opCall.docs[0]["credit"] -= opCall.args["amount"];
            opCall.docs[1]["credit"] += opCall.args["amount"];
            opCall.colls[0].add({"_id":"8", "log":"player bought"});
          })
      ]
   ];

  setUp(() {
    var mongoUrl = "mongodb://0.0.0.0/mongoProviderTest";
    var host = "127.0.0.1";
    var msPort = 27001;
    var lockerPort = 27002;
    updateLock = new DataReference(false);
    return LockRequestor.connect(host, lockerPort)
    .then((LockRequestor _lockRequestor) => lockRequestor = _lockRequestor)
    .then((_) => mongoConnection = new MongoConnection(mongoUrl, lockRequestor))
    .then((_) => mongoConnection.init())
    .then((_) => mongoServer = new MongoServer(msPort, mongoConnection))
    .then((_) => mongoServer.init())
    .then((_) => mongoConnection.transact((MongoDatabase mdb) => mdb.dropCollection(collectionName)))
    .then((_) {
      mongoClient = new MongoClient(host, msPort);

      MultiRequestHandler requestHandler = new MultiRequestHandler();
      requestHandler.registerHandler('sync-operation', mongoClient.handleSyncRequest);
      connection = new Connection.config(
          new LoopBackTransportStub(requestHandler.handleLoopBackRequest,null));
      transactor = new Transactor.config(connection, updateLock, "author", new IdGenerator());

      operations.forEach((e) {
        mongoServer.operations[e[0].name]= e[0];
        transactor.operations[e[1].name] = e[1];
      });
      mongoServer.registerBeforeCallback('addAll', allowOperation);
      mongoServer.registerBeforeCallback('change', allowOperation);
      mongoServer.registerBeforeCallback('removeAll', allowOperation);
      mongoServer.registerBeforeCallback('send money', allowOperation);
    });
  });

  tearDown(() {
    return Future.wait([mongoServer.close(), mongoClient.close(), lockRequestor.close()]);
  });

  _addCollectionName(List<Map> docs, collectionName) => docs..forEach((e) => e["__clean_collection"] = collectionName);

  test('Can send money if there\'s enough credit', () {
    DataMap first = new DataMap.from({"_id":"1", "name":"jozo","credit":5000});
    DataMap second = new DataMap.from({"_id":"2", "name":"fero", "credit":1000});
    DataSet logs = new DataSet();
    SubscriptionMock sub = new SubscriptionMock();
    sub.collection = logs;
    sub.mongoCollectionName = logCollectionName;
    Map args = {"amount":3000};
    return mongoConnection.collection(collectionName).addAll([first, second],'author')
      .then((_) {
        _addCollectionName([first,second],collectionName);
        return transactor.operation('send money', args, docs: [first, second], subs:[sub]);
      })
      .then((_) {
        expect(first["credit"], equals(2000));
        expect(second["credit"], equals(4000));
        return mongoConnection.collection(collectionName).find({"_id":"1"}).findOne();
      })
      .then((d) {
        expect(d["credit"], equals(2000));
        return mongoConnection.collection(collectionName).find({"_id":"2"}).findOne();
      })
      .then((d) {
        expect(d["credit"], equals(4000));
        return mongoConnection.collection(logCollectionName).find({"_id":"8"}).findOne();
      })
      .then((d) {
        expect(logs.elementAt(0)["log"], equals("player bought"));
        expect(d["log"], equals("player bought"));
      });
  });

  test('Should not send money if there\'s not enough credit', () {
    DataMap first = new DataMap.from({"_id":"1", "name":"jozo", "credit":2000});
    DataMap second = new DataMap.from({"_id":"2", "name":"fero", "credit":1000});
    DataSet logs = new DataSet();
    SubscriptionMock sub = new SubscriptionMock();
    sub.collection = logs;
    sub.mongoCollectionName = logCollectionName;
    Map args = {"amount" : 2048};
    return mongoConnection.collection(collectionName).addAll([first, second], 'author')
      .then((_) {
        _addCollectionName([first, second], collectionName);
        return transactor.operation('send money', args, docs: [first,second], subs:[sub]);
      })
      .then((_) => mongoConnection.collection(collectionName).find({"_id":"1"}).findOne())
      .then((d) {
        expect(d["credit"], equals(2000));
        return mongoConnection.collection(collectionName).find({"_id":"2"}).findOne();
      })
      .then((d) => expect(d["credit"], equals(1000)));
  });
}
