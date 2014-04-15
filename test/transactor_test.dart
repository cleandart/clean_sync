library client_test;
import 'package:unittest/unittest.dart';
import 'package:clean_ajax/client.dart';
import 'package:mock/mock.dart';
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

    setUp(() {
      failure = false;
      connection = new ConnectionMock();
      transactor = new Transactor(connection);
      months = new DataSet()..addAll(
                ["jan","feb","mar","apr","jun","jul","aug","sep","oct","nov","dec"]);

      transactor.registerOperation("save",
        operation: (fullDocs, args, DataSet collection) {
          collection.add(args);
        }
      );
      transactor.registerOperation("change",
        operation: (fullDocs, args, DataSet collection) {
          if (fullDocs is List) fullDocs = fullDocs[0];
          args.forEach((k,v) => fullDocs[k] = v);
        }
      );
      transactor.registerOperation("noop",
        before: (fullDocs, args, user, DataSet collection) => failure = true,
        after: (fullDocs, args, user, DataSet collection) => failure = true
      );
    });

    tearDown(() {
      months.clear();
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
      Map args = {"new" : "month"};
      List<Map> docs = [
          {"_id":"1", "haa":"ha", "__clean_collection":"weird"},
          {"_id":"2", "aee":"ug", "__clean_collection":"dummy"},
      ];
      return transactor.operation("change", {
        "collections" : [months,'months'],
        "docs" : docs,
        "args" : args
      }).then((_) =>
        expect(connection.getLogs(callsTo('send')).logs.first.args.first().toJson(),
            equals(new ClientRequest('sync-operation',{'operation':'change','args':{
              'docs':[['1','weird'],['2','dummy']],
              'collections':'months',
              'args':args}}).toJson()
            )
        )
      );
    });

    test("transactor shall not run \'before\' or \'after\' function", () {
      return transactor.operation("noop",{}).then((_) => expect(failure, isFalse));
    });

  });
}