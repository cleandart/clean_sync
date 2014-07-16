library clean_sync.mongo_server;

import 'package:clean_sync/server.dart';
import 'dart:async';
import 'dart:io';
import 'dart:convert';
import 'dart:math';
import 'package:logging/logging.dart';
import 'operations.dart' as ops;
import 'operations.dart';
import 'server_operations.dart' as sOps;
import 'package:clean_data/clean_data.dart';

Logger logger = new Logger('mongo_wrapper_logger');

class Tuple {
  var fst;
  var snd;
  Tuple(this.fst, this.snd);
}

Tuple decodeLeadingNum(String message) {
  // Take while it's a digit
  List codeUnits = message.codeUnits.takeWhile((c) => ((c >= 48) && (c <= 57))).toList();
  // If there are only digits, the leading number is problably not transfered whole
  if ((codeUnits.length == message.length) || (codeUnits.isEmpty)) return new Tuple(-1, -1);
  return new Tuple(num.parse(new String.fromCharCodes(codeUnits)), codeUnits.length);
}

/**
 * Takes a [message] of potentially concatenated JSONs
 * and returns List of separate JSONs. If the message is incomplete,
 * the incomplete part is stored in [incompleteJson]
 * */
List<String> getJSONs(String message, [Map incompleteJson]) {
  List<String> jsons = [];
  int messageLength = 0;
  int lastAdditionAt = 0;
  logger.finest("Messages: $message");
  logger.finest("From previous iteration: $incompleteJson");
  if (incompleteJson == null) incompleteJson = {};
  if (incompleteJson.containsKey("msg")) {
    // Previous JSON was not sent entirely
    message = incompleteJson["msg"] + message;
    logger.finest("New message: $message");
  }

  int i = 0;
  while (i < message.length) {
    // Beginning of new message
    // Performance upgrade, there's not going to be JSON longer than 10 bil chars..
    // Returns -1 if there are only digits or no digits
    // Assert = message[i] is a beginning of some valid message => the leading
    // few characters determine the length of message
    Tuple messageInfo = decodeLeadingNum(message.substring(i, i+10));
    messageLength = messageInfo.fst;
    if (messageLength == -1) {
      // Length of string was not sent entirely
      break;
    }
    i += messageInfo.snd;
    if (messageLength+i > message.length) {
      // We want to send more chars than this message contains =>
      // it was not sent entirely
      break;
    }
    jsons.add(message.substring(i, i+messageLength));
    lastAdditionAt = i+messageLength;
    i += messageLength;
  }
  if (lastAdditionAt != message.length-1) {
    // message is incomplete
    incompleteJson["msg"] = message.substring(lastAdditionAt);
  } else incompleteJson["msg"] = "";
  logger.fine("Jsons: $jsons");
  return jsons;
}

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

  ServerOperationCall(this.name, {this.docs, this.colls,
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
  Cache cache;
  Map <String, ServerOperation> operations = {};
  MongoDatabase db;
  String userColName;
  List<RawOperationCall> queue;
  List<Socket> clientSockets = [];
  ServerSocket serverSocket;
  Map incompleteJson = {};

  MongoServer(this.port, this.mongoUrl, {this.cache, this.userColName}){
    ops.commonOperations.forEach((o) => operations[o.name] = o);
    sOps.operations.forEach((o) => operations[o.name] = o);
  }

  Future start() {
    if (cache == null) db = new MongoDatabase(mongoUrl);
    else db = new MongoDatabase(mongoUrl, cache: cache);
    queue = [];
    incompleteJson = {};
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
      logger.finer("Received JSON: ${UTF8.decode(data)}");
      logger.finest("Char codes: ${data}");
      logger.finer("Incomplete json: $incompleteJson");
      // JSONs could have been sent frequently and therefore concatenated
      List<String> messages = getJSONs(UTF8.decode(data), incompleteJson);
      logger.finest('Messages: $messages');
      logger.finest("Incomplete json after: $incompleteJson");
      var jsons = messages.map((f) {
        try{
          JSON.decode(f);
        } catch (e){
          logger.shout("Failed to decode JSON from $f",e);
          throw e;
        }
        return JSON.decode(f);
      });
      logger.finer("Parsed JSONs: $jsons");
      List<RawOperationCall> opCalls = new List();
      jsons.forEach((m) {
        var op = new RawOperationCall.fromJson(m);
        opCalls.add(op);
        queue.add(op);
        op.completer.future.then((Map response){
          response['operationId'] = m['operationId'];
          String responseToSend = JSON.encode(response);
          socket.write('${responseToSend.length}${responseToSend}');
        });
      });
      _performOne();
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
//        before: before == null ? [] : [before], after: after == null ? [] : [after]);
  }

  registerBeforeCallback(operationName, before) {
    operations[operationName].before.add(before);
  }

  bool running = false;

  _performOne() {
    if (running) return;
    if (queue.isEmpty) return;
    logger.fine('server: perform one');
    running = true;
    _performOperation(queue.removeAt(0)).then((_) {
      running = false;
      _performOne();
    });
  }

  Future _performOperation(RawOperationCall opCall) {
    ServerOperation op = operations[opCall.name];
    List fullDocs = [];
    List fullColls = [];
    ServerOperationCall fOpCall;
    Map user;
    MongoProvider mongoProvider;

    for (String col in opCall.colls) {
      fullColls.add(db.collection(col));
    }

    logger.finer('fetching docs');
    int i = -1;
    return Future.forEach(opCall.docs, (doc){
      i++;
      return db.collection(opCall.docs[i][1]).find({'_id': opCall.docs[i][0]}).findOne()
          .catchError((e,s) => throw new DocumentNotFoundException('$e','$s'))
          .then((fullDoc) => fullDocs.add(fullDoc));
    }).then((_){
      logger.finer('Docs received: ${fullDocs}');
      logger.finer('fetching user');
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
      logger.finer('operation - before');
      user = _user != null ? new DataMap.from(_user) : null;
      fOpCall = new ServerOperationCall(opCall.name, docs: fullDocs,
          colls: fullColls, user: user, args: opCall.args, author: opCall.author,
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
        if (e == false) throw new ValidationException('Operation ${op.name} not permitted');
        // Some other error occured
        throw e;
      });
    }).then((_) {
      logger.finer('operation - core');
      return op.operation(fOpCall);
    }).then((_) {
      return Future.forEach(fullDocs, (d) {
        return db.collection(d["__clean_collection"]).change(d["_id"], d,
            opCall.author, clientVersion: opCall.clientVersion);
      });
    }).then((_) {
      logger.fine('operation - after');
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