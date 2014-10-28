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
import 'package:useful/socket_jsonizer.dart';
import 'package:clean_ajax/server.dart';

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

  RawOperationCall.fromRequest(ServerRequest request){
    name = request.args['operation'];
    args = request.args['args'];
    userId = request.authenticatedUserId;
    author = request.args['author'];
    clientVersion = request.args['clientVersion'];
    completer = new Completer();
    docs = request.args['docs'];
    if (docs == null) {
      docs = [];
    }
    colls = request.args['colls'];
    if (colls == null) {
      colls = [];
    }
  }
}


class TransactorServer {
  int port;
  String mongoUrl;
  Map <String, ServerOperation> operations = {};
  MongoConnection mongoConnection;
  String userColName;
  List<RawOperationCall> queue = [];

  // Assert: MongoConnection is already initialized
  TransactorServer(this.mongoConnection, {this.userColName}){
    ops.commonOperations.forEach((o) => operations[o.name] = o);
    sOps.operations.forEach((o) => operations[o.name] = o);
  }

  Future init() {
    return new Future.value(null);
  }

  Future handleSyncRequest(ServerRequest request) {
      _logger.finest("Request-operation: ${request.args}");
      List<RawOperationCall> opCalls = new List();
      var op = new RawOperationCall.fromRequest(request);
      opCalls.add(op);
      queue.add(op);
      _performOne();
      return op.completer.future;
    }

  Future close() {
     return new Future.value(null);
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
    if (running) return;
    if (queue.isEmpty) return;
    _logger.finer('server: perform one');
    running = true;
    _performOperationZoned(queue.removeAt(0)).then((_) {
      running = false;
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
      fullColls.add(mongoConnection.collection(col));
    }

    _logger.finest('fetching docs ($opCall)');
    int i = -1;
    return mongoConnection.transact((MongoDatabase mdb) => Future.forEach(opCall.docs, (doc){
      i++;
      return mongoConnection.collection(opCall.docs[i][1]).find({'_id': opCall.docs[i][0]}).findOne()
          .catchError((e,s) => throw new DocumentNotFoundException('$e','$s'))
          .then((fullDoc) => fullDocs.add(fullDoc));
    }).then((_){
      _logger.finest('Docs received: ${fullDocs} ($opCall)');
      _logger.finest('fetching user ($opCall)');
      if (opCall.userId != null) {
        if (userColName == null) {
          throw new Exception('userColName is not set!');
        }
        return mongoConnection.collection(userColName).find({'_id': opCall.userId}).findOne();
      } else {
        return null;
      }
    })
    .then((_user){
      _logger.finer('MS operation - before ($opCall)');
      user = _user != null ? new DataMap.from(_user) : null;
      fOpCall = new ServerOperationCall(opCall.name, docs: fullDocs,
          colls: fullColls, user: user, args: opCall.args, db: mdb, author: opCall.author,
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
        return mongoConnection.collection(d["__clean_collection"]).change(d["_id"], d,
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
    }), author: "TransactorServer. Operation name: ${opCall.name}");
  }
}