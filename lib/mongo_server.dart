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

Logger _logger = new Logger('mongo_wrapper_logger');

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
    return 'RawOperationCall name: $name, author: $author, docs: $docs, '
           'colls: $colls, args: $args, userId: $userId';
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
  Map <String, ServerOperation> operations = {};
  MongoDatabase db;
  String userColName;
  List<RawOperationCall> queue;
  List<Socket> clientSockets = [];
  ServerSocket serverSocket;

  Map<String, Map> locks = {};
  Map<String, List> requestors = {};

  MongoServer(this.port, this.db, {this.userColName}){
    ops.commonOperations.forEach((o) => operations[o.name] = o);
    sOps.operations.forEach((o) => operations[o.name] = o);
  }

  Future start() {
    queue = [];
    var socketFuture = ServerSocket.bind("127.0.0.1", port).then(
      (ServerSocket server) {
        serverSocket = server;
        serverSocket.listen(handleClient);
      }
    ).catchError((e,s) =>
        print("Caught error: $e, $s"));
    return Future.wait(db.init..add(socketFuture));
  }

  /**
   * Checks if somebody from requestors can receive a lock
   */
  checkLockRequestors() {
    requestors.forEach((coll, req) {
      if (!locks.containsKey(coll)) {
        // Nobody has lock for this coll
        if (requestors[coll].isNotEmpty) {
          // Somebody is waiting for this lock
          locks[coll] = requestors[coll].removeAt(0);
          writeJSON(locks[coll]["socket"], JSON.encode({"result": "ok", "id": locks[coll]["id"], "collection":coll, "action":"get"}));
        }
      }
    });
  }

  _addRequestor(String requestor, String collectionName, Socket socket) {
    if (!requestors.containsKey(collectionName)) requestors[collectionName] = [];
    requestors[collectionName].add({"id":requestor, "socket":socket});
    checkLockRequestors();
  }

  _releaseLock(String id, String collectionName, Socket socket) {
    if (locks.containsKey(collectionName)) {
      locks.remove(collectionName);
    }
    writeJSON(socket, JSON.encode({"id": id, "collection": collectionName, "result":"ok"}));
    _performOne();
    checkLockRequestors();
  }

  _releaseAllLocks(String id, String collectionName, Socket socket) {
    locks.clear();
    writeJSON(socket, JSON.encode({"id": id, "collection": collectionName, "result":"ok"}));
    _performOne();
    checkLockRequestors();
  }

  handleLock(Socket socket, Map req) =>
    (){
        switch(req["action"]) {
          case "get": return _addRequestor;
          case "release": return _releaseLock;
          case "releaseAll": return _releaseAllLocks;
        }
      }()(req["id"], req["collection"], socket);

  handleOperation(Socket socket, Map req) {
    List<RawOperationCall> opCalls = new List();
    var op = new RawOperationCall.fromJson(req);
    opCalls.add(op);
    queue.add(op);
    op.completer.future.then((Map response){
      response['operationId'] = req['operationId'];
      writeJSON(socket, JSON.encode(response));
    });
    _performOne();
  }

  handleClient(Socket socket){
    clientSockets.add(socket);
    socket.done.then((_) => clientSockets.remove(socket));
    toJsonStream(socket).listen((Map req) {
      if (req["type"] == "operation") return handleOperation(socket, req["data"]);
      if (req["type"] == "lock") return handleLock(socket, req["data"]);
    });
  }

  Future close() {
    return Future.wait([
       db.close(),
       Future.wait(clientSockets.map((socket) => socket.close())),
       serverSocket.close(),
    ]);
  }

  registerOperation(name, {operation, before, after}){
    _logger.fine("registering operation $name");
    operations[name] = new ServerOperation(name, operation: operation,
        before: before, after: after);
//        before: before == null ? [] : [before], after: after == null ? [] : [after]);
  }

  registerBeforeCallback(operationName, before) {
    operations[operationName].before.add(before);
  }

  bool running = false;

  _performOne() {
    if (locks.isNotEmpty) return;
    if (running) return;
    if (queue.isEmpty) return;
    _logger.finer('server: perform one');
    running = true;
    _performOperationZoned(queue.removeAt(0)).then((_) {
      running = false;
      checkLockRequestors();
      _performOne();
    });
  }

  Future _performOperationZoned(RawOperationCall opCall){
    return runZoned((){
      return _performOperation(opCall).then((result) {
        (Zone.current[#db_lock]['stopwatch'] as Stopwatch).stop();
        (Zone.current[#db_lock]['stopwatchAll'] as Stopwatch).stop();

        int elapsed = (Zone.current[#db_lock]['stopwatch'] as Stopwatch).elapsedMilliseconds;
        int elapsedAll = (Zone.current[#db_lock]['stopwatchAll'] as Stopwatch).elapsedMilliseconds;

        if(elapsed > 200) {
          _logger.warning('Operaration lasted $elapsed milliseconds (totaly $elapsedAll)'
              '${opCall}');
        }
        return result;
      });
    }, zoneValues: {#db_lock: {'count': 0, 'stopwatch': new Stopwatch()..start(), 'stopwatchAll': new Stopwatch()..start()}});

  }

  Future _performOperation(RawOperationCall opCall) {
    ServerOperation op = operations[opCall.name];
    if(op == null) {
      opCall.completer.complete({'error':{'Unknown operation':'${opCall.name}'}});
      _logger.shout('Unknown operation ${opCall.name}');
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

    _logger.finest('fetching docs ($opCall)');
    int i = -1;
    return Future.forEach(opCall.docs, (doc){
      i++;
      return db.collection(opCall.docs[i][1]).find({'_id': opCall.docs[i][0]}).findOne()
          .catchError((e,s) => throw new DocumentNotFoundException('$e','$s'))
          .then((fullDoc) => fullDocs.add(fullDoc));
    }).then((_){
      _logger.finest('Docs received: ${fullDocs} ($opCall)');
      _logger.finest('fetching user ($opCall)');
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
      _logger.finer('MS operation - before ($opCall)');
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
        if (e == false) {
          throw new ValidationException('Validation failed');
        }
        // Some other error occured
        throw e;
      });
    }).then((_) {
      _logger.finer('MS operation - core ($opCall)');
      return op.operation(fOpCall);
    }).then((_) {
      return Future.forEach(fullDocs, (d) {
        return db.collection(d["__clean_collection"]).change(d["_id"], d,
            opCall.author, clientVersion: opCall.clientVersion);
      });
    }).then((_) {
      _logger.finer('MS operation - after ($opCall)');
      return Future.forEach(op.after, (opAfter) => opAfter(fOpCall));
    }).then((_) {
      opCall.completer.complete({'result': 'ok'});
    }).catchError((e, s) {
      if (e is ValidationException) {
        _logger.warning('Validation failed: Operation ${op.name} not'
                'permitted; user: ${fOpCall.user}, author: ${fOpCall.author},'
                'docs: ${fOpCall.docs}, colls: ${fOpCall.colls}', e,  s);
        opCall.completer.complete({'error':{'validation':'$e'}});
      } else if (e is DocumentNotFoundException) {
        _logger.warning('Document not found', e, s);
        opCall.completer.complete({'error':{'doc_not_found':'$e'}});
      } else {
        _logger.shout("Some other error occured !",e,s);
        opCall.completer.complete({'error':{'unknown':'$e $s'}});
      }
    });
  }
}