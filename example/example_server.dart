import 'package:clean_sync/server.dart';
import 'package:clean_backend/clean_backend.dart';
import 'dart:async';
import 'package:clean_ajax/server.dart';
import 'package:crypto/crypto.dart';
import 'package:clean_router/common.dart';

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
  mongodb.createIndex('persons', {'name': 1}, unique: true);
  Future.wait(mongodb.init).then((_) {

    publish('persons', (_) {
      return mongodb.collection("persons");
    });

    publish('personsOlderThan24', (_) {
      return mongodb.collection("persons").find({"age" : {'\$gt' : 24}});
      //return mongodb.collection("persons").find({"age" : null});
    });

    publish('personsOlderThan24Desc', (_) {
      return mongodb.collection("persons").find({"age" : {'\$gt' : 24}}).sort({"age": DESC}).skip(2).limit(3);
      //return mongodb.collection("persons").find({"age" : null});
    });

    Backend.bind('0.0.0.0', 8080, []).then((backend) {
      backend.router.addRoute("static", new Route('/static/*'));
      backend.router.addRoute("resources", new Route('/resources/'));
      MultiRequestHandler requestHandler = new MultiRequestHandler();
      requestHandler.registerDefaultHandler(handleSyncRequest);
      backend.addStaticView('static', './');
      backend.addView('resources', requestHandler.handleHttpRequest);
    });
  });
}
