import 'package:clean_sync/server.dart';
import 'package:clean_backend/clean_backend.dart';
import 'package:static_file_handler/static_file_handler.dart';
import 'package:clean_ajax/server.dart';

void main(){
  var mongo = new MongoProvider('mongodb://127.0.0.1:27017/clean');
  publish('persons', (_) {
    return mongo.collection("persons");
  });
    publish('personsOlderThan24', (_) {
    return mongo.collection("persons").find({"age" : {'\$gt' : 24}});
  });
  StaticFileHandler fileHandler = new StaticFileHandler.serveFolder('/home/marcelka/projects/clean_sync/web/');
  RequestHandler requestHandler = new RequestHandler();
  requestHandler.registerDefaultExecutor(handleSyncRequest);
  mongo.initialize(['persons']).then((_) {
      new Backend(fileHandler, requestHandler, host: '127.0.0.1', port: 8080)..listen();
  });
}