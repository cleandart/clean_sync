library clean_sync.mongo_server;

import 'package:clean_sync/server.dart';
import 'dart:async';
import 'dart:io';
import 'dart:convert';
import 'package:logging/logging.dart';
import 'operations.dart' as ops;
import 'operations.dart';

Logger logger = new Logger('mongo_wrapper_logger');

emptyFun(){}

/**
 * Takes a [message] of potentially concatenated JSONs
 * and returns List of separate JSONs. If the message is incomplete,
 * the incomplete part is stored in [incompleteJson]
 * */
List<String> getJSONs(String message, [Map incompleteJson]) {
  List<String> jsons = [];
  int numl = 0;
  String temp = "";
  int startPoint = 0;
  logger.fine("Messages: $message");
  logger.fine("From previous iteration: $incompleteJson");
  if (incompleteJson == null) incompleteJson = {};
  if (incompleteJson.containsKey("numl")) {
    numl = incompleteJson["numl"];
    message = incompleteJson["msg"] + message;
    startPoint = incompleteJson["msg"].length;
    temp = incompleteJson["msg"];
    logger.fine("New message: $message");
  }
  int lastAdditionAt = 0;
  for (int i = startPoint; i < message.length; i++) {
    temp += message[i];
    if (message[i] == '{') numl++;
    if (message[i] == '}') numl--;
    if (numl == 0) {
      jsons.add(temp);
      lastAdditionAt = i;
      temp = "";
    }
  }
  if (lastAdditionAt != message.length-1) {
    // message is incomplete
    incompleteJson["numl"] = numl;
    incompleteJson["msg"] = message.substring(lastAdditionAt+1);
  }
  logger.fine("Jsons: $jsons");
  return jsons;
}

class DocumentNotFoundException implements Exception {
  final String error;
  final String stackTrace;
  DocumentNotFoundException(this.error, [this.stackTrace]);
  String toString() => error;
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
  String author;
  String clientVersion;

  OperationCall(this.name, this.completer, {this.docs, this.collections,
    this.args, this.userId, this.author, this.clientVersion});

  OperationCall.fromJson(Map source){
    name = source['name'];
    args = source['args'];
    userId = source['userId'];
    author = source['author'];
    clientVersion = source['clientVersion'];
    completer = new Completer();

    if (source['docs'] == null) {
      docs = [];
      docsListed = false;
    } else {
      if (source['docs'][0] is! List) {
        docs = [source['docs']];
        docsListed = true;
      } else {
        docs = source['docs'];
        docsListed = false;
      }
    }

    if (source['collections'] == null) collections = [];
    else if (source['collections'] is! List) {
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
  // {numl:[int], msg:[String]}
  Map incompleteJson = {};

  MongoServer(this.port, this.mongoUrl){
    ops.commonOperations.forEach((o) => operations[o.name] = o);
    ops.incompatibleOperations.forEach((o) => operations[o[0].name] = o[0]);
  }

  MongoServer.config(this.port, this.mongoUrl);


  Future start() {
    db = new MongoDatabase(mongoUrl);
    queue = [];
    incompleteJson = {"numl":0, "msg":""};
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
      logger.finer("Received JSON: ${new String.fromCharCodes(data)}");
      logger.finer("Incomplete json: $incompleteJson");
      // JSONs could have been sent frequently and therefore concatenated
      List<String> messages = getJSONs(new String.fromCharCodes(data), incompleteJson).map((f) => JSON.decode(f));
      logger.fine("Parsed JSONs: $messages");
      List<OperationCall> opCalls = new List();
      messages.forEach((m) {
        var op = new OperationCall.fromJson(m);
        opCalls.add(op);
        queue.add(op);
        op.completer.future.then((Map response){
          response['operationId'] = m['operationId'];
          socket.write(JSON.encode(response));
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
        before: before == null ? [] : [before], after: after == null ? [] : [after]);
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

  Future _performOperation(OperationCall opCall) {
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

    return Future.forEach(opCall.docs, (doc){
      logger.fine('fetching docs');
      i++;
      return db.collection(opCall.docs[i][1]).find({'_id': opCall.docs[i][0]}).findOne()
          .catchError((e,s) => throw new DocumentNotFoundException('$e','$s'))
          .then((fullDoc) => fullDocs.add(fullDoc));
    }).then((_){
      logger.fine('Docs received: ${fullDocs}');
      logger.fine('fetching user');
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
      logger.fine('operation - before');
      user = _user;
      for (String col in opCall.collections) {
        fullColls.add(db.collection(col));
      }
      if (fullDocs.isEmpty) fullDocsArg = null;
      else fullDocsArg = opCall.docsListed ? fullDocs[0] : fullDocs;
      if (fullColls.isEmpty) fullCollsArg = null;
      else fullCollsArg = opCall.collectionsListed ? fullColls[0] : fullColls;
      return Future.forEach(op.before, (b) => reduceArguments(b, fullDocsArg, opCall.args, user, fullCollsArg)());
    }).then((_) {
      logger.fine('operation - core');
      return reduceArguments(op.operation, fullDocsArg, opCall.args, null, fullCollsArg)()
        .then((_) {
          if (fullDocsArg != null) {
            if (fullDocsArg is! List) fullDocsArg = [fullDocsArg];
            return Future.forEach(fullDocsArg, (d) {
              return db.collection(d["__clean_collection"]).change(d["_id"], d,
                  opCall.author, clientVersion: opCall.clientVersion);
            });
          } else return null;
      });
    }).then((_) {
      logger.fine('operation - after');
      return Future.forEach(op.after, (a) => reduceArguments(a, fullDocsArg, opCall.args, user, fullCollsArg)());
    }).then((_) {
      opCall.completer.complete({'result': 'ok'});
    }).catchError((e, s) {
      if (e is ValidationException) {
        logger.warning('Validation failed', e,  s);
        opCall.completer.complete({'error':{'validation':'$e'}});
      } else if (e is DocumentNotFoundException) {
        logger.warning('Document not found', e, s);
        opCall.completer.complete({'error':{'query':'$e'}});
      } else {
        logger.shout("Some other error occured !",e,s);
        opCall.completer.complete({'error':{'unknown':'$e'}});
      }
    });
  }

}