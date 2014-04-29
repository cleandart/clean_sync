import 'package:clean_sync/mongo_server.dart';
import 'package:clean_sync/mongo_client.dart';
import 'package:clean_sync/server.dart';
import 'package:useful/useful.dart';
import 'package:logging/logging.dart';
import 'dart:io';

Logger logger = new Logger('mongo_wrapper_logger');

main(){

  setupDefaultLogHandler();
  logger.level = Level.FINER;

  MongoServer server = new MongoServer(27001, "mongodb://0.0.0.0/mongoServerTest");
  server.start();
  server.registerOperation("save",
      operation: (fullDocs, args, MongoProvider collection){
        return collection.add(args, "");
      }
  );
  server.registerOperation("delete",
      operation: (fullDocs, args, MongoProvider collection) {
        return collection.remove(args["_id"],"");
      }
  );

  int idgen = new DateTime.now().millisecondsSinceEpoch;
  MongoClient client = new MongoClient("127.0.0.1", 27001);

  // Create 2 and delete 1
  client.connected.then((_){
    idgen++;
    return client._performOperation('save', collections: 'test', args: {'_id' : '$idgen', 'name': 'jozo'});
  }).then((result){
    print(result);
    client = new MongoClient("127.0.0.1", 27001);
    client.connected.then((_) {
      return client._performOperation('save', collections: 'test', args: {'_id' : '${idgen+1}' , 'name' : 'juro'});
    }).then((result) => print(result));
    MongoClient client2 = new MongoClient("127.0.0.1", 27001);
    client2.connected.then((_) {
      return client2._performOperation('delete', collections: 'test', args: {'_id':'$idgen'});
    }).then((result) => print(result));
  });

}