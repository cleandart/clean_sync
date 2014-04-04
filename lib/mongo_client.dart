library clean_sync.mongo_client;

import 'package:clean_sync/mongo_server.dart';
import 'package:clean_sync/server.dart';
import 'dart:io';
import 'dart:math';
import 'dart:async';
import 'dart:convert';
import 'package:logging/logging.dart';


Logger logger = new Logger('mongo_wrapper_logger');

class MongoClient {

  String url;
  int port;
  Socket socket;
  int count = 0;
  Completer _connected;
  Future get connected => _connected.future;


  String prefix = (new Random(new DateTime.now().millisecondsSinceEpoch % (1<<20))).nextDouble().toString();
  Map<String, Completer> reqToResp = {};

  MongoClient(this.url, this.port){
    _connected = new Completer();
    Socket.connect(url, port)
        .then((Socket _socket) {
          _connected.complete(null);
          socket = _socket;
          socket.listen((List <int> data){
            Map resp = JSON.decode(new String.fromCharCodes(data));
            Completer completer = reqToResp.remove(resp['operationId']);
            if (resp.containsKey('result')) {
              completer.complete(resp['result']);
            } else
            if (resp.containsKey('error')) {
              completer.completeError(resp['error']);
            } else {
              completer.completeError('MongoClient - unknown error');
            }
          });
        })
        .catchError((e) {
          print("Unable to connect: $e");
          exit(1);
        });
  }

  Future performOperation(name, {docs, collection, args, userId}) {
    Completer completer = new Completer();
    String operationId = '$prefix--${count++}';
    reqToResp[operationId] = completer;
    socket.write(JSON.encode({'name': name, 'docs': docs, 'collection': collection, 'args': args,
      'userId': userId, 'operationId': operationId}));
    return completer.future;
  }
}

