import 'package:clean_sync/server.dart';
import 'package:clean_backend/clean_backend.dart';
import 'dart:async';
import 'package:clean_ajax/server.dart';
import 'package:crypto/crypto.dart';



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
      //return mongodb.collection("persons").find({"age" : null});
   });
    Backend.bind([], new SHA256()).then((backend) {
      MultiRequestHandler requestHandler = new MultiRequestHandler();
      requestHandler.registerDefaultHandler(handleSyncRequest);
      backend.addDefaultHttpHeader('Access-Control-Allow-Origin','*');
      backend.addView(r'/resources', requestHandler.handleHttpRequest);
      backend.addStaticView(new RegExp(r'/.*'), '../web/');
    });
  });
}
