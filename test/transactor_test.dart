library transactor_test;
import 'package:unittest/unittest.dart';
import 'package:clean_ajax/client.dart';
import 'package:mock/mock.dart';
import 'package:clean_sync/client.dart';
import 'package:clean_data/clean_data.dart';
import 'package:useful/useful.dart';

class ConnectionMock extends Mock implements Connection {}
class IdGeneratorMock extends Mock implements IdGenerator {}

void main() {
  run();
}

void run() {
  group("Transactor", (){

    ConnectionMock connection;
    Transactor transactor;
    DataSet months;
    DataReference updateLock = new DataReference(null);
    bool failure;

    setUp(() {
      failure = false;
      connection = new ConnectionMock();
      transactor = new Transactor(connection, updateLock, 'author', new IdGeneratorMock());
      months = new DataSet()..addAll(
                ["jan","feb","mar","apr","jun","jul","aug","sep","oct","nov","dec"]);

      transactor.registerClientOperation("save",
          operation: (args, {collection}) {
            collection.add(args);
          });


    });

    tearDown(() {
      months.clear();
    });

    test("transactor saves document", () {
      Map args = new DataMap.from({"name":"random document", "_id":"1"});
      transactor.performClientOperation("save",
          args,
          collection: [months,'months']
      );
      expect(months.contains(args), isTrue);

    });

    test("transactor sends the right data", () {
      Map args = new DataMap.from({"new" : "month", "_id":"2"});
      List<Map> docs = [
          {"_id":"1", "haa":"ha", "__clean_collection":"weird"},
          {"_id":"2", "aee":"ug", "__clean_collection":"dummy"},
      ];
      return transactor.performServerOperation("change", args, docs:docs)
        .then((_) {
        Map actual = connection.getLogs(callsTo('send')).logs.first.args.first().toJson();
        actual["args"] = slice(actual["args"], ["docs","args","operation"]);
        expect(actual,
            equals(new ClientRequest('sync-operation',{'operation':'change',
              'docs':[['1','weird'],['2','dummy']],
              'args':args}).toJson()
            )
        );
      }
      );
    });

  });
}