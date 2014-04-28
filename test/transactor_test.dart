library transactor_test;
import 'package:unittest/unittest.dart';
import 'package:unittest/mock.dart';
import 'package:clean_ajax/client.dart';
import 'package:clean_sync/client.dart';
import 'package:clean_data/clean_data.dart';

class ConnectionMock extends Mock implements Connection {}

void main() {
  run();
}

void run() {
  group("Transactor", (){

    ConnectionMock connection;
    Transactor transactor;
    DataSet months;
    bool failure;
    DataMap january;
    DataMap february;
    var mockOperColls, mockOperArgs, mockOperDocs;


    setUp(() {
      failure = false;
      connection = new ConnectionMock();
      transactor = new Transactor(connection);

      months = new DataSet()..addAll(
                ["jan","feb","mar","apr","jun","jul","aug","sep","oct","nov","dec"]);

      january = new DataMap.from({'__clean_collection': 'months', '_id': '1', 'name': 'january'});
      february = new DataMap.from({'__clean_collection': 'months', '_id': '2', 'name': 'february'});

      transactor.registerClientOperation('mockOper',
        operation: (args, {docs, colls}) {
          mockOperColls = colls;
          mockOperArgs = args;
          mockOperDocs = docs;
        }
      );

//      transactor.registerClientOperation("save",
//        operation: (docs, args, DataSet collection) {
//          collection.add(args);
//        }
//      );
//
//      transactor.registerClientOperation("change",
//        operation: (fullDocs, args, DataSet collection) {
//          if (fullDocs is List) fullDocs = fullDocs[0];
//          args.forEach((k,v) => fullDocs[k] = v);
//        }
//      );

    });

    tearDown(() {
      months.clear();
    });

    solo_test('transactor performs client operation', (){
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
                    'colls': ['colors', 'animals']
                  }
             }));

    });

    test("transactor saves document", () {
      Mock args = new Mock();
      return transactor.operation("save", {
        "collections":[months,'months'],
        "args": args
        }
      ).then((_) => expect(months.contains(args), isTrue));
    });

    test("transactor sends the right data", () {
      Map args = {"arg_key" : "arg_val"};
      List<Map> docs = [
          {"_id":" 1", "key1": "val1", "__clean_collection": "col1"},
          {"_id": "2", "key2": "val2", "__clean_collection": "col2"},
      ];
      return transactor.operation("change", {
        "collections" : [months,'months'],
        "docs" : docs,
        "args" : args
      }).then((_) =>
        expect(connection.getLogs(callsTo('send')).logs.first.args.first().toJson(),
            equals(new ClientRequest('sync-operation',{'operation':'change','args':{
              'docs':[['1','col1'],['2','col2']],
              'collections':'months',
              'args':args}}).toJson()
            )
        )
      );
    });

  });
}