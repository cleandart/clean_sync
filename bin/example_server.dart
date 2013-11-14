import 'package:clean_sync/server.dart';
import 'package:clean_backend/clean_backend.dart';
import 'package:static_file_handler/static_file_handler.dart';
import 'dart:async';
import 'package:clean_ajax/server.dart';


void main() {
  /**
   * Mongo daemon has to be running at its default port.
   * No authentification is used (/etc/mongodb.conf contains auth=false, which
   * is default value).
   * If authentification would be used:
   * url = 'mongodb://clean:clean@127.0.0.1:27017/clean';
   */
  MongoDatabase mongodb = new MongoDatabase('mongodb://127.0.0.1:27017/clean');
  mongodb.create_collection('persons');
  Future.wait(mongodb.init).then((_) {
    publish('persons', (_) {
      return mongodb.collection("persons");
    });
      publish('personsOlderThan24', (_) {
      return mongodb.collection("persons").find({"age" : {'\$gt' : 24}});
    });
    StaticFileHandler fileHandler =
        new StaticFileHandler.serveFolder('../web/');
    MultiRequestHandler requestHandler = new MultiRequestHandler();
    requestHandler.registerDefaultHandler(handleSyncRequest);
    new Backend(fileHandler, requestHandler, host: '127.0.0.1', port: 8080)
        ..listen();
  });
}
