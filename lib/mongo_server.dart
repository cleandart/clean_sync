library clean_sync.mongo_server;

import 'package:clean_sync/server.dart';
import 'dart:async';
import 'dart:io';
import 'dart:convert';
import 'package:logging/logging.dart';

Logger logger = new Logger('mongo_wrapper_logger');

emptyFun(){}

class ValidationException implements Exception {
  final String error;
  final String stackTrace;
  ValidationException(this.error, [this.stackTrace]);
  String toString() => error;
}

class ServerOperation {
  String name;
  Function before;
  Function operation;
  Function after;

  ServerOperation(this.name, {this.operation, this.before,
      this.after});
}

class OperationCall {
  String name;
  List<List> docs;
  List<String> collections;
  Map args;
  String userId;
  Completer completer;
  bool docsListed;
  bool collectionsListed;

  OperationCall(this.name, this.completer, {this.docs, this.collections,
    this.args, this.userId});

  OperationCall.fromJson(Map source){
    name = source['name'];
    args = source['args'];
    userId = source['userId'];
    completer = new Completer();

    if (source['docs'] is! List<List>) {
      docs = [source['docs']];
      docsListed = true;
    } else {
      docs = source['docs'];
      docsListed = false;
    }
    if (source['docs'] == null) {
      docs = [];
      docsListed = false;
    }

    if (source['collections'] is! List) {
      collections = [source['collections']];
      collectionsListed = true;
    } else {
      collections = source['collections'];
      collectionsListed = false;
    }
  }
}


class MongoServer{
  int port;
  String mongoUrl;
  Map <String, ServerOperation> operations = {};
  MongoDatabase db;
  String userColName;
  List<OperationCall> queue;
  List<Socket> clientSockets = [];
  ServerSocket serverSocket;

  MongoServer(this.port, this.mongoUrl);

  Future start() {
    db = new MongoDatabase(mongoUrl);
    queue = [];
    var socketFuture = ServerSocket.bind("127.0.0.1", port).then(
      (ServerSocket server) {
        serverSocket = server;
        server.listen(handleClient);
      }
    );
    return Future.wait(db.init..add(socketFuture));
  }

  handleClient(Socket socket){
    clientSockets.add(socket);
    socket.listen((List<int> data){
      logger.info("Received JSON: ${new String.fromCharCodes(data)}");
      Map message = JSON.decode(new String.fromCharCodes(data));
      OperationCall opCall = new OperationCall.fromJson(message);
      queue.add(opCall);
      performOne();
      opCall.completer.future.then((Map response){
        response['operationId'] = message['operationId'];
        socket.write(JSON.encode(response));
       // socket.close();
      });
    });
  }

  Future close() {
    return Future.wait([
       db.close(),
       Future.wait(clientSockets.map((socket) => socket.close())),
       serverSocket.close()
    ]);
  }

  registerOperation(name, {operation, before, after}){
    logger.fine("registering operation $name");
    operations[name] = new ServerOperation(name, operation: operation,
        before: before, after: after);
  }

  performOne() {
    logger.fine('perform one');
    if (queue.isEmpty) return;
    new Future.delayed(new Duration(), (){
      if (queue.isEmpty) return;
      _performOperation(queue.removeAt(0));
      performOne();
    });
  }

  _performOperation(OperationCall opCall) {
    ServerOperation op = operations[opCall.name];
    List fullDocs = [];
    List fullColls = [];
    int i = -1;
    Map user;
    MongoProvider mongoProvider;
    bool _docsListed;
    bool _colsListed;
    var fullDocsArg;
    var fullCollsArg;

    Future.forEach(opCall.docs, (doc){
      logger.fine('fetching docs');
      i++;
      return db.collection(opCall.docs[i][1]).find({'_id': opCall.docs[i][0]}).findOne()
          .then((fullDoc) => fullDocs.add(fullDoc));
    }).then((_){
      logger.fine('Docs received: ${fullDocs}');
      logger.fine('fetching user');
      if (opCall.userId != null) {
        print('user id: ${opCall.userId}');
        if (userColName == null) {
          throw new Exception('userColName is not set!');
        }
        return db.collection(userColName).find({'_id': opCall.userId}).findOne();
      } else {
        return null;
      }
    })
    .then((_user){
      logger.fine('operation - before');
      user = _user;
      for (String col in opCall.collections) {
        fullColls.add(db.collection(col));
      }
      fullDocsArg = opCall.docsListed ? fullDocs[0] : fullDocs;
      fullCollsArg = opCall.collectionsListed ? fullColls[0] : fullColls;
      return op.before == null ? null: op.before(fullDocsArg,
          opCall.args, user, fullCollsArg);
    }).then((_) {
      logger.fine('operation - core');
      return op.operation == null ? null : op.operation(fullDocsArg, opCall.args, fullCollsArg);
    }).then((_) {
      logger.fine('operation - after');
      return op.after == null ? null : op.after(fullDocsArg, opCall.args, user, fullCollsArg);
    }).then((_) {
      opCall.completer.complete({'result': 'ok'});
    }).catchError((e, s) {
      if (e is ValidationException) {
        opCall.completer.complete({'error':{'validation':'$e'}});
      } else {
        opCall.completer.complete({'error':{'unknown':'$e'}});
      }
    });
  }

}