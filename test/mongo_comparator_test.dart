library mongo_provider_test;

import "package:unittest/unittest.dart";
import "package:clean_sync/server.dart";
import "dart:async";

main() {
  MongoProvider collection;
  MongoDatabase mongodb;

  setup() {
    mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest');
    return Future.wait(mongodb.init)
    .then((_) => mongodb.dropCollection('sort_test'))
    .then((_) => mongodb.removeLocks())
    .then((_){
      collection = mongodb.collection('sort_test');
    });
  }

  Future _test(List<Map> input_to_sort, Map sort_params, List<Map> expected_output){
    Completer completer = new Completer();

    Future.forEach(input_to_sort, ((Map entry) => collection.add(entry, "test"))).then((_) {
      print("all was added");
      collection.find({}).sort(sort_params).data().then((Map data) {
        List<Map<String, dynamic>> given_output = data['data'];
        print("given_output: " + given_output.toString().replaceAll("_id", "\n_id"));

        expect(given_output.length, equals(expected_output.length));

        for(int i=0; i<given_output.length ; i++){
          // remove additional keys starting with _
          Map polished_map = {};
          given_output[i].forEach((String key, value){
            if(!key.startsWith("_")){
              polished_map[key] = value;
            }
          });

          expect(polished_map, equals(expected_output[i]));
        }

        completer.complete(given_output);
      });
    });

    return completer.future;
  };

  _teardown() {
    mongodb.close();
  };

  Future runTest(List<Map> input_to_sort, Map sort_params, List<Map> expected_output) {
    return setup()
      .then((_) => _test(input_to_sort, sort_params, expected_output))
      .then((_) => _teardown());
  }

  test('Integer sorting.', () {
    List<Map> input_to_sort =
    [
      {'a' : 5},
      {'a' : 1},
      {'a' : 3},
      {'a' : 2},
      {'a' : 4},
    ];

    List<Map> expected_output =
    [
      {'a' : 1},
      {'a' : 2},
      {'a' : 3},
      {'a' : 4},
      {'a' : 5},
    ];


    return runTest(input_to_sort, {'a' : 1}, expected_output);
  });
}
