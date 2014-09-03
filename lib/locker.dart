library clean_sync.locker;

import 'dart:io';
import 'dart:async';
import 'package:clean_sync/clean_stream.dart';
import 'dart:convert';
import 'package:logging/logging.dart';

Logger _logger = new Logger('clean_sync.locker');

class Locker {

  ServerSocket serverSocket;
  List<Socket> clientSockets = [];
  List<Map> requestors = [];
  Map currentLock = {};
  Completer _done;

  Future get closed => _done.future;

  Locker.config(this.serverSocket);

  static Future<Locker> bind(url, port) =>
      ServerSocket.bind(url, port)
        .then((ServerSocket sSocket) {
          Locker locker = new Locker.config(sSocket);
          locker.serverSocket.listen(locker.handleClient);
          locker._done = new Completer();
          return locker;
        });

  handleClient(Socket socket) {
    clientSockets.add(socket);
    socket.done.then((_) => clientSockets.remove(socket));
    toJsonStream(socket).listen((Map data) {
      if (data["type"] == "lock") handleLockRequest(data["data"], socket);
    });
  }

  handleLockRequest(Map req, Socket socket) =>
    (req["action"] == "get" ? _addRequestor : _releaseLock)(req["id"], socket);

  _addRequestor(String id, Socket socket) {
    if (requestors == null) requestors = [];
    requestors.add({"id": id, "socket": socket});
    checkLockRequestors();
  }

  _releaseLock(String id, Socket socket) {
    currentLock.remove("lock");
    writeJSON(socket, JSON.encode({"result":"ok", "action":"release", "id": id}));
    checkLockRequestors();
  }

  checkLockRequestors() {
    if (!currentLock.containsKey("lock") && requestors.isNotEmpty) {
      currentLock["lock"] = requestors.removeAt(0);
      writeJSON(currentLock["lock"]["socket"], JSON.encode({"result":"ok", "action":"get", "id": currentLock["lock"]["id"]}));
    }
  }

  Future close() =>
     Future.wait([
       Future.wait(clientSockets.map((s) => s.close())),
       serverSocket.close()
     ]).then((_) => _done.complete());

}