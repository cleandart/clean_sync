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

int ab = 35;
var selectors = [
{"\$query":{"\$or":[{"\$and":[{"version":{"\$gt":ab}},{"before":{"\$gt":{}}},{"before.a":"hello"}]},{"\$and":[{"version":{"\$gt":ab}},{"after":{"\$gt":{}}},{"after.a":"hello"}]}],"version":{"\$gt":ab}}},
{"\$query":{"\$or":[{"\$and":[{"version":{"\$gt":ab}},{"before":{"\$gt":{}}},{"before.a":"hello"}]},{"\$and":[{"version":{"\$gt":ab}},{"after":{"\$gt":{}}},{"after.a":"hello"}]}],"version":{"\$gt":ab}},"\$orderby":{"version":1}}
];


main() {

  MongoDatabase mongodb;
  MongoServer mongoServer = new MongoServer(27001, "mongodb://0.0.0.0/mongoProviderTest");
  return mongoServer.start()
      .then((_) {
        mongodb = mongoServer.db;
        MongoProvider provider = mongodb.collection("random");
        return Future.forEach(selectors, (selector) {
            return provider.collectionHistory.find(selector).toList()
               .then((data) {
                  print(selector);
                  print('');

                  data.forEach((e) => print(e));

                  print('');
              });
        });
  }).then((_) => mongoServer.close());

}