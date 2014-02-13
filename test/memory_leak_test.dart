import "package:unittest/unittest.dart";
import "package:clean_sync/server.dart";
import "dart:async";
import './mongo_provider_test.dart';
import 'package:clean_sync/client.dart';
import 'package:unittest/mock.dart';
import 'package:clean_ajax/client.dart';
import 'package:clean_ajax/client_backend.dart';
import 'package:clean_ajax/server.dart';
import 'package:clean_data/clean_data.dart';
import 'package:logging/logging.dart';

main(){
  unittestConfiguration.timeout = null;
  hierarchicalLoggingEnabled = true;
  Logger.root.level = Level.WARNING;
//  (new Logger('clean_sync')).level = Level.FINEST;
//  Logger.root.level = Level.FINE;
  Logger.root.onRecord.listen((LogRecord rec) {
    print('${rec.loggerName} ${rec.message} ${rec.error} ${rec.stackTrace}');
  });
  run();
}

run() {
  MongoDatabase mongodb;
  Connection connection;
  DataSet colAll;
  Subscription subAll;

  Publisher pub;


  setUp((){
    mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest');

    return Future.wait(mongodb.init)
    .then((_) => mongodb.dropCollection('random'))
    .then((_) => mongodb.removeLocks()).then((_){
        pub = new Publisher();
        pub.publish('a', (_) {
          return mongodb.collection("random").find({});
        }
        , versionProvider: mongodb.collection("random")
        );

        MultiRequestHandler requestHandler = new MultiRequestHandler();
        requestHandler.registerDefaultHandler(pub.handleSyncRequest);
        connection = createLoopBackConnection(requestHandler);

    });
  });

  solo_test('big data performance', () {
    var data = new DataMap();
    for(int i=0; i<2000; i++) {
      data['$i'] = {'key' : i};
    }
    num i=-1;
    var idGen = new IdGenerator('a');
    return
      mongodb.dropCollection('random').then((_) =>
      mongodb.removeLocks()).then((_) =>

      Future.forEach(new List.filled(100, null), (_) {
        subAll = new Subscription('a', connection, 'author1', idGen, {});
        colAll = subAll.collection;
        data.remove('_id');
        return subAll.initialSync.then((_) =>
          colAll.removeAll(new Set.from(colAll))
        ).then((_) =>
          colAll.add(data)
        ).then((_) =>
          Future.forEach(new List.filled(2000, null), (_) {
            print(++i);
//            print(data);
            data['${i%1000}']['key']='changed $i';
            return new Future.delayed(new Duration(milliseconds: 3));
          })).then((_) => subAll.close());
      }));
  });

}