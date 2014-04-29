library transactor_test;
import 'package:unittest/unittest.dart';
import 'package:unittest/mock.dart';
import 'package:clean_ajax/client.dart';
import 'package:clean_sync/client.dart';
import 'package:clean_sync/mongo_server.dart';
import 'package:clean_data/clean_data.dart';
import 'package:useful/useful.dart';

class ConnectionMock extends Mock implements Connection {}
class IdGeneratorMock extends Mock implements IdGenerator {}

void main() {
  run();
}

void run() {
  group("Transactor basics", (){

    ConnectionMock connection;
    Transactor transactor;
    DataSet months;
    DataReference updateLock = new DataReference(null);
    DataMap january;
    DataMap february;
    var mockOperColls, mockOperArgs, mockOperDocs;


    setUp(() {
      connection = new ConnectionMock();
      IdGeneratorMock idGenerator = new IdGeneratorMock();
      idGenerator.when(callsTo("next")).thenReturn("uniqueId");

      transactor = new Transactor(connection, updateLock, 'author', idGenerator );
      january = new DataMap.from({'__clean_collection': 'months', '_id': '1', 'name': 'january'});
      february = new DataMap.from({'__clean_collection': 'months', '_id': '2', 'name': 'february'});

      transactor.registerClientOperation('mockOper',
        operation: (args, {docs, colls}) {
          mockOperColls = colls;
          mockOperArgs = args;
          mockOperDocs = docs;
        }
      );

    });

    test('transactor performs client operation', (){
      DataSet colors = new DataSet();
      DataSet animals = new DataSet();
      Map args = {'args': 'args'};

      transactor.operation('mockOper', {'args': 'args'}, docs: [january, february],
          colls: [[colors, 'colors'], [animals, 'animals']]);
      expect(mockOperArgs, equals(args));
      expect(mockOperColls, equals([colors, animals]));
      expect(mockOperDocs, equals([january, february]));

      expect(connection.getLogs(callsTo('send')).logs.first.args.first().toJson(),
          equals({'type': 'sync-operation',
                  'args': {
                    'operation':'mockOper',
                    'args': {'args': 'args'},
                    'docs': [['1','months'],['2','months']],
                    'colls': ['colors', 'animals'],
                    'author': 'author',
                    'clientVersion': 'uniqueId'
                  }
             }));
    });

  });

}