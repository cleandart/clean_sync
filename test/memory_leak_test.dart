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
  (new Logger('clean_sync')).level = Level.WARNING;
  Logger.root.onRecord.listen((LogRecord rec) {
    print('${rec.loggerName} ${rec.message} ${rec.error} ${rec.stackTrace}');
  });
  run();
}

run() {
  MongoDatabase mongodb;
  DataSet colAll;
  DataSet colAll2;
  DataSet colA;
  DataSet colAa;
  DataSet colMapped;

  Connection connection;
  Subscription subAll;
  Subscription subAll2;
  Subscription subA;
  Subscription subAa;
  Subscription subMapped;

  DataMap data1;
  DataMap data2;
  DataMap dataA;

  Publisher pub;


  setUp((){
    mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest');

    return Future.wait(mongodb.init)
    .then((_) => mongodb.dropCollection('random'))
    .then((_) => mongodb.removeLocks()).then((_){
        pub = new Publisher();
        pub.publish('a', (_) {
          return mongodb.collection("random").find({});
        });

        MultiRequestHandler requestHandler = new MultiRequestHandler();
        requestHandler.registerDefaultHandler(pub.handleSyncRequest);
        connection = createLoopBackConnection(requestHandler);

    });
  });

  solo_test('big data performance', () {
    var data = new DataMap();
    for(int i=0; i<10; i++) {
      print('init $i');
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
//        colAll = new DataSet();
        data.remove('_id');
        return subAll.initialSync.then((_) =>
          colAll.removeAll(new Set.from(colAll))
        ).then((_) =>
          colAll.add(data)
        ).then((_) =>
          Future.forEach(new List.filled(10, null), (_) {
            print(++i);
//            print(data);
            data['${i%1}']['key']='changed $i';
            return new Future.delayed(new Duration(milliseconds: 200));
          })).then((_) => subAll.close());
      }));
  });

}