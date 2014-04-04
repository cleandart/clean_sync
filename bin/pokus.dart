import 'package:clean_sync/mongo_server.dart';
import 'package:clean_sync/mongo_client.dart';
import 'package:clean_sync/server.dart';
import 'package:useful/useful.dart';
import 'package:logging/logging.dart';
import 'dart:io';

Logger logger = new Logger('mongo_wrapper_logger');

main(){

  setupDefaultLogHandler();
  logger.level = Level.FINE;

  MongoServer server = new MongoServer(27001, "mongodb://0.0.0.0/mongoServerTest");
  server.start();
  server.registerOperation("save",
      operation: (fullDocs, args, MongoProvider collection){
        return collection.add(args, "");
      }
  );
  MongoClient client = new MongoClient("127.0.0.1", 27001);

  client.connected.then((_){
    return client.performOperation('save', collection: 'test', args: {'name': 'jozo'});
  }).then((result){
    print(result);
  });

}