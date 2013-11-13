import 'package:clean_sync/server.dart';
import 'package:clean_backend/clean_backend.dart';
import 'package:static_file_handler/static_file_handler.dart';
import 'package:mongo_dart/mongo_dart.dart';
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
  Db db = new Db('mongodb://127.0.0.1:27017/clean');
  Future conn = db.open();

  var mongo = new MongoProvider(db, conn);
  publish('persons', (_) {
    return mongo.collection("persons");
  });
    publish('personsOlderThan24', (_) {
    return mongo.collection("persons").find({"age" : {'\$gt' : 24}});
  });
  StaticFileHandler fileHandler =
      new StaticFileHandler.serveFolder('/home/marcelka/projects/clean_sync/web/');
  RequestHandler requestHandler = new RequestHandler();
  requestHandler.registerDefaultExecutor(handleSyncRequest);
  new Backend(fileHandler, requestHandler, host: '127.0.0.1', port: 8080)
      ..listen();
}
