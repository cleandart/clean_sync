library clean_sync.mongo_client;

import 'package:clean_sync/mongo_server.dart';
import 'package:clean_sync/server.dart';
import 'dart:io';
import 'dart:math';
import 'dart:async';
import 'dart:convert';
import 'package:logging/logging.dart';
import 'package:clean_ajax/server.dart';


Logger logger = new Logger('mongo_wrapper_logger');

class MongoClient {

  String url;
  int port;
  Socket socket;
  int count = 0;
  Completer _connected;
  List<Function> queue = [];
  Future get connected => _connected.future;
  Map incompleteJson = {};


  String prefix = (new Random(new DateTime.now().millisecondsSinceEpoch % (1<<20))).nextDouble().toString();
  Map<String, Completer> reqToResp = {};

  MongoClient.config(this.url, this.port) {
    _connected = new Completer();
  }

  MongoClient(this.url, this.port){
    _connected = new Completer();
    this.connect();
  }

  //connect

  Future connect() =>
    Socket.connect(this.url, this.port)
        .then((Socket _socket) {
          _connected.complete(null);
          socket = _socket;
          socket.listen((List <int> data){
            logger.finer('Raw response: ${new String.fromCharCodes(data)}');
            // We could have received more JSONs at once
            var responses = getJSONs(new String.fromCharCodes(data), incompleteJson).map((m) => JSON.decode(m));
            logger.finer("JSON resp: $responses");
            responses.forEach((resp) {
              Completer completer = reqToResp.remove(resp['operationId']);
              if (resp.containsKey('result')) {
                completer.complete(resp['result']);
              } else if (resp.containsKey('error')) {
                completer.completeError(resp['error']);
              } else {
                completer.completeError('MongoClient - unknown error');
              }
            });
          });
        })
        .catchError((e) {
          print("Unable to connect: $e");
          exit(1);
        });


  Future handleSyncRequest(ServerRequest request) {
    Map data = request.args;
    logger.finest("Request-operation: $data");
    Map args = data["args"];
    return performOperation(data['operation'], docs:args["docs"],
        collections:args["collections"],args:args["args"], userId:request.authenticatedUserId);
  }

  Future performOperation(name, {docs, collections, args, userId}) {
    Completer completer = new Completer();
    String operationId = '$prefix--${count++}';
    reqToResp[operationId] = completer;
    logger.finer("ReqToResp: ${reqToResp}");
    socket.write(JSON.encode({'name': name, 'docs': docs, 'collections': collections, 'args': args,
      'userId': userId, 'operationId': operationId}));
    return completer.future;
  }
}

