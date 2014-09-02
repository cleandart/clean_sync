library clean_sync.mongo_server;

import 'package:clean_sync/server.dart';
import 'dart:async';
import 'dart:io';
import 'dart:convert';
import 'package:logging/logging.dart';
import 'operations.dart' as ops;
import 'operations.dart';
import 'server_operations.dart' as sOps;
import 'package:clean_data/clean_data.dart';
import 'package:clean_sync/clean_stream.dart';

Logger logger = new Logger('mongo_wrapper_logger');

class DocumentNotFoundException implements Exception {
  final String error;
  final String stackTrace;
  DocumentNotFoundException(this.error, [this.stackTrace]);
  String toString() => error;
}

class ServerOperationCall extends CommonOperationCall {
  String name;
  List<DataMap> docs;
  List<MongoProvider> colls;
  Map args;
  DataMap user;
  String author;
  String clientVersion;
  MongoDatabase db;

  ServerOperationCall(this.name, {this.docs, this.colls, this.db,
    this.args, this.user, this.author, this.clientVersion});

}

class RawOperationCall {
  String name;
  List<List<String>> docs;
  List<String> colls;
  Map args;
  String userId;
  Completer completer;
  String author;
  String clientVersion;

  @override
  String toString(){
    return "RawOperationCall $name ${super.toString()}";
  }

  RawOperationCall(this.name, this.completer, {this.docs, this.colls,
    this.args, this.userId, this.author, this.clientVersion});

  RawOperationCall.fromJson(Map source){
    name = source['name'];
    args = source['args'];
    userId = source['userId'];
    author = source['author'];
    clientVersion = source['clientVersion'];
    completer = new Completer();
    docs = source['docs'];
    if (docs == null) {
      docs = [];
    }
    colls = source['colls'];
    if (colls == null) {
      colls = [];
    }
  }
}


class MongoServer {
  int port;
  String mongoUrl;
  int locksPort = 27002;
  Cache cache;
  Map <String, ServerOperation> operations = {};
  MongoDatabase db;
  String userColName;
  List<RawOperationCall> queue;
  List<Socket> clientSockets = [];
  ServerSocket serverSocket;
  ServerSocket locksSocket;

  Map<String, Map> locks = {};
  Map<String, List> requestors = {};

  MongoServer(this.port, this.mongoUrl, {this.cache, this.userColName}){
    ops.commonOperations.forEach((o) => operations[o.name] = o);
    sOps.operations.forEach((o) => operations[o.name] = o);
  }

  Future start() {
    if (cache == null) db = new MongoDatabase(mongoUrl);
    else db = new MongoDatabase(mongoUrl, cache: cache);
    queue = [];
    var socketFuture = ServerSocket.bind("127.0.0.1", port).then(
      (ServerSocket server) {
        serverSocket = server;
        server.listen(handleClient);
      }
    ).catchError((e,s) =>
        print("Caught error: $e, $s"));
    var mongodbSocketFuture = ServerSocket.bind("127.0.0.1", locksPort)
        .then((ServerSocket mdbServer) {
      locksSocket = mdbServer;
      locksSocket.listen(handleLocksClient);
    });
    return Future.wait(db.init..addAll([socketFuture, mongodbSocketFuture]));
  }

  checkLockRequestors() {
    requestors.forEach((coll, req) {
      if (!locks.containsKey(coll)) {
        // Nobody has lock for this coll
        if (requestors[coll].isNotEmpty) {
          // Somebody is waiting for this lock
          locks[coll] = requestors[coll].removeAt(0);
          String stringToSend = JSON.encode({"result": "ok", "id": locks[coll]["id"]});
          (locks[coll]["socket"] as Socket).write("${stringToSend.length}${stringToSend}");
        }
      }
    });
  }

  _addRequestor(String requestor, String collectionName, Socket socket) {
    if (!requestors.containsKey(collectionName)) requestors[collectionName] = [];
    requestors[collectionName].add({"id":requestor, "socket":socket});
    checkLockRequestors();
  }

  // Releases lock for collectionName and returns the id that held the lock
  String _releaseLock(String collectionName) {
    if (!locks.containsKey(collectionName)) return null;
    String id = locks.remove(collectionName)["id"];
    checkLockRequestors();
    return id;
  }

  handleLocksClient(Socket socket) {
    clientSockets.add(socket);
    toJsonStream(socket).listen((Map req) {
      if (req["operation"] == "get") _addRequestor(req["id"], req["collection"], socket);
      if (req["operation"] == "release") _releaseLock(req["collection"]);
    });
  }

  handleClient(Socket socket){
    clientSockets.add(socket);
    toJsonStream(socket).listen((Map json) {
      List<RawOperationCall> opCalls = new List();
        var op = new RawOperationCall.fromJson(json);
        opCalls.add(op);
        queue.add(op);
        op.completer.future.then((Map response){
          response['operationId'] = json['operationId'];
          writeJSON(socket, JSON.encode(response));
        });
      _performOne();
    });
  }

  Future close() {
    print("trying to close");
    return Future.wait([
       db.close(),
       Future.wait(clientSockets.map((socket) => socket.close())),
       serverSocket.close(),
       locksSocket.close(),
    ]).then((_) => print("closed"));
  }

  registerOperation(name, {operation, before, after}){
    logger.fine("registering operation $name");
    operations[name] = new ServerOperation(name, operation: operation,
        before: before, after: after);
//        before: before == null ? [] : [before], after: after == null ? [] : [after]);
  }

  registerBeforeCallback(operationName, before) {
    operations[operationName].before.add(before);
  }

  bool running = false;

  _performOne() {
    if (running) return;
    if (queue.isEmpty) return;
    logger.finer('server: perform one');
    running = true;
    _performOperation(queue.removeAt(0)).then((_) {
      running = false;
      _performOne();
    });
  }

  Future _performOperation(RawOperationCall opCall) {
    ServerOperation op = operations[opCall.name];
    if(op == null) {
      opCall.completer.complete({'error':{'Unknown operation':'${opCall.name}'}});
      logger.shout('Unknown operation ${opCall.name}');
      return new Future(() => null);
    }
    List fullDocs = [];
    List fullColls = [];
    ServerOperationCall fOpCall;
    Map user;
    MongoProvider mongoProvider;

    for (String col in opCall.colls) {
      fullColls.add(db.collection(col));
    }

    logger.finest('fetching docs ($opCall)');
    int i = -1;
    return Future.forEach(opCall.docs, (doc){
      i++;
      return db.collection(opCall.docs[i][1]).find({'_id': opCall.docs[i][0]}).findOne()
          .catchError((e,s) => throw new DocumentNotFoundException('$e','$s'))
          .then((fullDoc) => fullDocs.add(fullDoc));
    }).then((_){
      logger.finest('Docs received: ${fullDocs} ($opCall)');
      logger.finest('fetching user ($opCall)');
      if (opCall.userId != null) {
        if (userColName == null) {
          throw new Exception('userColName is not set!');
        }
        return db.collection(userColName).find({'_id': opCall.userId}).findOne();
      } else {
        return null;
      }
    })
    .then((_user){
      logger.finer('operation - before ($opCall)');
      user = _user != null ? new DataMap.from(_user) : null;
      fOpCall = new ServerOperationCall(opCall.name, docs: fullDocs,
          colls: fullColls, user: user, args: opCall.args, db: db, author: opCall.author,
          clientVersion: opCall.clientVersion);
      return Future.forEach(op.before, (opBefore) =>
        // Before callbacks should return either true, false or null
        // True: OK, False: Not OK, Null: nothing
        // First explicit result decides if the operation is permitted
        // Future type return is supported - although not required
        new Future(() => opBefore(fOpCall)).then((result) {
          if (result == true) throw true;
          if (result == false) throw false;
          return true;
        })
      ).then((_) =>
          // No explicit result was thrown, defaults to NOT Permitted
          throw false
      ).catchError((e,s) {
        if (e == true) return true;
        if (e == false) throw new ValidationException('Operation ${op.name} not'
        'permitted; user: ${fOpCall.user}, author: ${fOpCall.author}, docs: ${fOpCall.docs},'
        'colls: ${fOpCall.colls}');
        // Some other error occured
        throw e;
      });
    }).then((_) {
      logger.finer('operation - core ($opCall)');
      return op.operation(fOpCall);
    }).then((_) {
      return Future.forEach(fullDocs, (d) {
        return db.collection(d["__clean_collection"]).change(d["_id"], d,
            opCall.author, clientVersion: opCall.clientVersion);
      });
    }).then((_) {
      logger.finer('operation - after ($opCall)');
      return Future.forEach(op.after, (opAfter) => opAfter(fOpCall));
    }).then((_) {
      opCall.completer.complete({'result': 'ok'});
    }).catchError((e, s) {
      if (e is ValidationException) {
        logger.warning('Validation failed', e,  s);
        opCall.completer.complete({'error':{'validation':'$e'}});
      } else if (e is DocumentNotFoundException) {
        logger.warning('Document not found', e, s);
        opCall.completer.complete({'error':{'doc_not_found':'$e'}});
      } else {
        logger.shout("Some other error occured !",e,s);
        opCall.completer.complete({'error':{'unknown':'$e $s'}});
      }
    });
  }

}