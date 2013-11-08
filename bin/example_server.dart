import 'package:clean_sync/server.dart';
import 'package:clean_backend/clean_backend.dart';
import 'package:static_file_handler/static_file_handler.dart';

void main(){
  var mongo = new MongoProvider('mongodb://clean:clean@127.0.0.1:27017/clean');
  publish('persons', (_) {
    return mongo.collection("persons");
  });
    publish('personsOlderThan24', (_) {
    return mongo.collection("persons").find({"age" : {'\$gt' : 24}});
  });
  StaticFileHandler fileHandler = new StaticFileHandler.serveFolder('/home/marcelka/');
  RequestHandler requestHandler = new RequestHandler();
  requestHandler.registerExecutor('', handleSyncRequest);
  mongo.initialize(['persons']).then((_) {
      new Backend(fileHandler, requestHandler, host: '127.0.0.1', port: 8080)..listen();
  });
}