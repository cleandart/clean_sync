import "package:unittest/unittest.dart";
import "package:mock/mock.dart";
import "package:clean_sync/server.dart";
import "dart:async";
import 'dart:math';
import './mongo_provider_test.dart';
import 'package:clean_sync/client.dart';
import 'package:clean_ajax/client.dart';
import 'package:clean_ajax/client_backend.dart';
import 'package:clean_ajax/server.dart';
import 'package:clean_data/clean_data.dart';
import 'package:logging/logging.dart';
import 'package:useful/useful.dart';
import 'package:clean_sync/mongo_server.dart';
import 'package:clean_sync/mongo_client.dart';
import 'package:clean_sync/id_generator.dart';

var selector = {"\$query":{"\$or":[{"\$and":[{"version":{"\$gt":3501}},{"before":{"\$gt":{}}},{"before.a.a":"hello"}]},{"\$and":[{"version":{"\$gt":3501}},{"after":{"\$gt":{}}},{"after.a.a":"hello"}]}],"version":{"\$gt":3501}},"\$orderby":{"version":1}};
main() {
  print(selector);
  MongoDatabase mongodb;
  MongoServer mongoServer = new MongoServer(27001, "mongodb://0.0.0.0/mongoProviderTest");
  return mongoServer.start()
      .then((_) {
        mongodb = mongoServer.db;
        MongoProvider provider = mongodb.collection("random");
        return provider.collectionHistory.find(selector).toList()
           .then((data) {
              data.forEach((e) => print(e));
          });
  }).then((_) => mongoServer.close());

}