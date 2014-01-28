library mongo_provider_test;

import "package:unittest/unittest.dart";
import "package:clean_sync/server.dart";
import "package:clean_sync/client.dart";
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

  Future _test(List<Map> input_to_sort, Map sort_params){
    Completer completer = new Completer();

    // sort with MongoDb
    Future.forEach(input_to_sort, ((Map entry) => collection.add(entry, "test"))).then((_) {
      print("all was added");

      // sort with MongoComparator
      //   ! is done here as only here "__clean_version" is added
      List<Map> given_output = new List.from(input_to_sort);
      given_output.sort((a,b) => MongoComparator.compareWithKeySelector(a,b, sort_params));

      collection.find({}).sort(sort_params).data().then((Map data) {
        List<Map<String, dynamic>> expected_output = data['data'];
        print("input_to_sort: \n" + input_to_sort.join("\n"));
        print("expected_output: \n" + expected_output.join("\n"));
        print("given_output: \n" + given_output.join("\n"));

        expect(expected_output.length, equals(given_output.length));

        // compare the two sortin methods
        for(int i=0; i<expected_output.length ; i++){
          expect(expected_output[i]['__clean_version'], equals(given_output[i]['__clean_version']));
        }

        completer.complete(expected_output);
      });
    });

    return completer.future;
  };

  _teardown() {
    mongodb.close();
  };

  Future runTest(List<Map> input_to_sort, Map sort_params) {
    return setup()
      .then((_) => _test(input_to_sort, sort_params))
      .then((_) => _teardown());
  }

  test('dummy compare.', () {

    expect(MongoComparator.compareWithKeySelector(
        {"a": null, "__clean_version": 3},
        {"a": [], "__clean_version": 7},
        {"a":1}), equals(1));
    expect(MongoComparator.compare(
        [[null], null],
        [null],
        inList:false), equals(0));
    expect(MongoComparator.compareWithKeySelector(
        {"a": [[null], null], "__clean_version": 3},
        {"a": [null], "__clean_version": 7},
        {"a":1}), equals(-1));
    expect(MongoComparator.compareWithKeySelector(
        {"a": [[]], "__clean_version": 3},
        {"a": [null], "__clean_version": 7},
        {"a":1}), equals(1));

    expect(MongoComparator.compareWithKeySelector(
        {"a": [[[]]], "__clean_version": 3},
        {"a": [[null]], "__clean_version": 7},
        {"a":1}), equals(1));
  });

  test('minListElement.', () {
    expect(MongoComparator.getListMinElement(
      [null]), equals([MongoComparator.TYPE_NULL, null]));
    expect(MongoComparator.getListMinElement(
      [[null], null]), equals([MongoComparator.TYPE_NULL, null]));
    expect(MongoComparator.getListMinElement(
      [[], 1, "abc", null]), equals([MongoComparator.TYPE_NULL, null]));
    expect(MongoComparator.getListMinElement(
      [[[]], [1], ["abc"], []]), equals([MongoComparator.TYPE_LIST, []]));
  });

  test('Integer sorting.', () {
    List<Map> input_to_sort =
    [
      {'a' : 5},
      {'a' : 1},
      {'a' : 3},
      {'a' : 2},
      {'a' : 4},
    ];

    return runTest(input_to_sort, {'a' : 1});
  });

  test('String sorting.', () {
    List<Map> input_to_sort =
    [
      {'a' : "e"},
      {'a' : "a"},
      {'a' : "b"},
      {'a' : "d"},
      {'a' : "c"},
    ];

    return runTest(input_to_sort, {'a' : 1});
  });

  test('Hierarchy.', () {
    List<Map> input_to_sort =
    [
      {'a' : []},
      {'a' : null},
      {'a' : 1},
      {'a' : 1.2},
      {'a' : "abc"},
      {'a' : {}},
      {'a' : [[]]},
      {'a' : false},
      //{'a' : new DateTime(2014)},
      //{'a' : new RegExp("[0-9]")},
    ];

    return runTest(input_to_sort, {'a' : 1});
  });

  test('List.', () {
    List<Map> input_to_sort =
    [
      {'a' : null},
      {'a' : 1},
      {'a' : 1.2},
      {'a' : "abc"},
      {'a' : {}},
      {'a' : false},
      {'a' : [null]},
      {'a' : [1]},
      {'a' : [1.2]},
      {'a' : ["abc"]},
      {'a' : [{}]},
      {'a' : [false]},
      {'a' : []},
    ];

    return runTest(input_to_sort, {'a' : 1});
  });

  test('Nested list.', () {
    List<Map> input_to_sort =
    [
      {'a' : null},
      {'a' : []},
      {'a' : [[null], null]},
      {'a' : [[null]]},
      {'a' : [[[]]]},
      {'a' : [[]]},
      {'a' : [null]},
    ];

    return runTest(input_to_sort, {'a' : 1});
  });

/*
> db.a.insert({'a': [[[1]], [1], 1]})
> db.a.insert({'a': [[[1]], [1], null]})
> db.a.insert({'a': [[[1]], [1], []]})
> db.a.insert({'a': [[null], null]})
> db.a.insert({'a': [[null], []]})
> db.a.insert({'a': [[1], [2], [3], [4], null]})
> db.a.insert({'a': [[[]]]})
> db.a.insert({'a': [[]]})
> db.a.insert({'a': [1]})
> db.a.insert({'a': [null]})
     *
{ "_id" : ObjectId("52deaaf349b3a947c24b4ada"), "a" : [  [  [  1 ] ],  [  1 ],  null ] }
{ "_id" : ObjectId("52deab0849b3a947c24b4adc"), "a" : [  [  null ],  null ] }
{ "_id" : ObjectId("52deab2849b3a947c24b4ade"), "a" : [  [  1 ],  [  2 ],  [  3 ],  [  4 ],  null ] }
{ "_id" : ObjectId("52deab9f49b3a947c24b4ae2"), "a" : [  null ] }
{ "_id" : ObjectId("52deaaf049b3a947c24b4ad9"), "a" : [  [  [  1 ] ],  [  1 ],  1 ] }
{ "_id" : ObjectId("52deab3d49b3a947c24b4ae1"), "a" : [  1 ] }
{ "_id" : ObjectId("52deaaf749b3a947c24b4adb"), "a" : [  [  [  1 ] ],  [  1 ],  [ ] ] }
{ "_id" : ObjectId("52deab1749b3a947c24b4add"), "a" : [  [  null ],  [ ] ] }
{ "_id" : ObjectId("52deab3a49b3a947c24b4ae0"), "a" : [  [ ] ] }
{ "_id" : ObjectId("52deab3449b3a947c24b4adf"), "a" : [  [  [ ] ] ] }
     *
     */

  test('Several element list.', () {
    List<Map> input_to_sort =
    [
      {'a' : [[[1]], [1], 1]},
      {'a' : [[[1]], [1], null]},
      {'a' : [[[1]], [1], []]},
      {'a' : [[null], null]},
      {'a' : [[null],[]]},
      {'a' : [[1], [2], [3], [4], null]},
      {'a' : [[[]]]},
      {'a' : [1]},
      {'a' : [null]},
      {'a' : [[]]},
    ];

    return runTest(input_to_sort, {'a' : 1});
  });

  test('Boolean.', () {
    List<Map> input_to_sort =
    [
      {'a' : true},
      {'a' : false},
      {'a' : true},
      {'a' : false},
      {'a' : true},
      {'a' : false},
    ];

    return runTest(input_to_sort, {'a' : 1});
  });

  test('Nested value', () {

    List<Map> input_to_sort =
    [
      {'a' : [[[4]]]},
      {'a' : [[[3]]]},
      {'a' : [[[2]]]},
      {'a' : [[[1]]]},
    ];

    return runTest(input_to_sort, {'a' : 1});
  });

  test('Nesting depth', () {

    List<Map> input_to_sort =
    [
      {'a' : [[[[[[]]]]]]},
      {'a' : [[[[[]]]]]},
      {'a' : [[[[]]]]},
      {'a' : [[[]]]},
      {'a' : [[]]},
      {'a' : []},
    ];

    return runTest(input_to_sort, {'a' : 1});
  });
  // TODO several keys to sort
}
