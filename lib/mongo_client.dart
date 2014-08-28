library clean_sync.mongo_client;

import 'package:clean_sync/mongo_server.dart';
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
  int _count = 0;
  Completer _connected;
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
            var responses = getJSONs(UTF8.decode(data), incompleteJson).map((m) => JSON.decode(m));
            logger.finer("JSON resp: $responses");
            responses.forEach((resp) {
              logger.fine('response obtained: ${resp}');
              Completer completer = reqToResp.remove(resp['operationId']);
              // Distinguish (un)successful operations by the key
              if (resp.containsKey('result')) {
                completer.complete(resp);
              } else if (resp.containsKey('error')) {
                //TODO: think about this
                completer.complete(resp);
              } else {
                completer.complete('MongoClient - unknown error');
              }
            });
          });
        })
        .catchError((e) {
          logger.shout("Unable to connect: $e");
          exit(1);
        });


  Future handleSyncRequest(ServerRequest request) {
    Map data = request.args;
    logger.finest("Request-operation: $data");
    return performOperation(data['operation'], docs: data["docs"],
        colls: data["colls"], args: data["args"],
        userId: request.authenticatedUserId, author: data["author"],
        clientVersion: data["clientVersion"]);
  }

  Future performOperation(name, {docs, colls, args, userId, author, clientVersion}) {
    Completer completer = new Completer();
    String operationId = '$prefix--${_count++}';
    reqToResp[operationId] = completer;
    logger.finer("ReqToResp: ${reqToResp}");
    String stringToSend = JSON.encode({'name': name, 'docs': docs, 'colls': colls, 'args': args,
      'userId': userId, 'operationId': operationId, 'author': author, 'clientVersion': clientVersion});
    logger.finest("Trying to send string: $stringToSend");
    socket.write('${stringToSend.length}${stringToSend}');
    return completer.future;
  }

  Future close() {
    if(socket == null) return new Future.value(null);
    return socket.close();
  }
}
