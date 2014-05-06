// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library mongo_provider_test;

import "package:unittest/unittest.dart";
import "package:clean_sync/server.dart";
import "package:useful/useful.dart";
import "dart:async";
import 'package:clean_data/clean_data.dart';


void handleDiff(List<Map> diff, List collection) {
  diff.forEach((Map change) {
    if (change["action"] == "add") {
      collection.add(clone(change["data"]));
    }
    else if (change["action"] == "change") {
      Map record = collection.firstWhere((d) => d["_id"] == change["_id"], orElse: ()=> null);
      if (record != null) {
        collection.remove(record);
        collection.add(change["data"]);
      }
    }
    else if (change["action"] == "remove") {
      collection.removeWhere((d) => d["_id"] == change["_id"]);
    }
  });
}

void main() {
  group('MongoProvider', () {
    MongoProvider months;
    Future ready;
    MongoDatabase mongodb;
    Map january, february, march, april, may, june, july,
        august, september, october, november, december;
    List monthsCol;

     setUp(() {
      january = {'name': 'January', 'days': 31, 'number': 1, '_id': 'january'};
      february = {'name': 'February', 'days': 28, 'number': 2, '_id': 'february'};
      march =  {'name': 'March', 'days': 31, 'number': 3, '_id': 'march'};
      april = {'name': 'April', 'days': 30, 'number': 4, '_id': 'april'};
      may = {'name': 'May', 'days': 31, 'number': 5, '_id': 'may'};
      june = {'name': 'June', 'days': 30, 'number': 6, '_id': 'june'};
      july = {'name': 'July', 'days': 31, 'number': 7, '_id': 'july'};
      august = {'name': 'August', 'days': 31, 'number': 8, '_id': 'august'};
      september = {'name': 'September', 'days': 30, 'number': 9, '_id': 'september'};
      october = {'name': 'October', 'days': 31, 'number': 10, '_id': 'october'};
      november = {'name': 'November', 'days': 30, 'number': 11, '_id': 'november'};
      december = {'name': 'December', 'days': 31, 'number': 12, '_id': 'december'};

      monthsCol = [january, february, march, april, may, june,
                    july, august, september, october, november, december];

      mongodb = new MongoDatabase('mongodb://127.0.0.1/mongoProviderTest');
      ready = Future.wait(mongodb.init).then((_) => mongodb.dropCollection('months'))
                    .then((_) => mongodb.removeLocks())
                    .then((_)=> mongodb.createIndex('months', {'a':1}, unique: true))
                    .then((_) => months = mongodb.collection('months'));
      return ready;
    });

    tearDown(() {
      mongodb.close();
    });

    test('get data. (T01)', () {
      // when
      return ready.then((_) {

        // then
        return months.data().then((Map data) {
          expect(data['data'], equals([]));
          expect(data['version'], equals(0));
        });
      });
    });

    test('add data. (T02)', () {
      // when
      return ready.then((_) => months.add(clone(january), 'John Doe'))
        .then((_) => months.data())
        .then((data){

      // then
          expect(data['data'].length, equals(1));
          Map strippedData = data['data'][0];
          expect(strippedData, equals(january));
          expect(data['version'], equals(1));
      }).then((_) => months.diffFromVersion(0))
        .then((dataDiff) {
          List diffList = dataDiff['diff'];
          expect(diffList.length, equals(1));
          Map diff = diffList[0];
          expect(diff['action'], equals('add'));
          expect(diff['_id'], equals('january'));
          Map strippedData = diff['data'];
          expect(strippedData, equals(january));
          expect(diff['author'], equals('John Doe'));
        });
    });

    test('add more data at once data. (T02.1)', () {
      // when
      return ready.then((_) => months.addAll([clone(january), clone(february)], 'John Doe'))
        .then((_) => months.data())
        .then((data){

      // then
          expect(data['data'].length, equals(2));
          Map strippedData = data['data'][0];
          expect(strippedData, equals(january));
          strippedData = data['data'][1];
          expect(strippedData, equals(february));

          expect(data['version'], equals(2));
      }).then((_) => months.diffFromVersion(0))
        .then((dataDiff) {
          List diffList = dataDiff['diff'];
          expect(diffList.length, equals(2));
          Map diff = diffList[0];
          expect(diff['action'], equals('add'));
          expect(diff['_id'], equals('january'));
          Map strippedData = diff['data'];
          expect(strippedData, equals(january));

          diff = diffList[1];
          expect(diff['action'], equals('add'));
          expect(diff['_id'], equals('february'));
          strippedData = diff['data'];
          expect(strippedData, equals(february));

          expect(diff['author'], equals('John Doe'));
        });
    });

    test('find', () {
      // when
      return ready.then((_) => months.addAll([clone(january), clone(february),
                                              clone(march), clone(april)], 'John Doe'))
        .then((_) => months.find({'days': 31}).data())
        .then((data){
          expect(data['data'], unorderedEquals([january, march]));
        });
    });

    test('take_fields', () {
      // when
      return ready.then((_) => months.addAll([clone(january), clone(february),
                                              clone(march), clone(april)], 'John Doe'))
        .then((_) => months.find({'days': 31}).fields(['days']).data())
        .then((data){
          expect(data['data'], unorderedEquals([{'days': 31, '_id': 'january'},
                                                {'days': 31, '_id': 'march'}]));
        });
    });

    test('exclude_fields', () {
      // when
      return ready.then((_) => months.addAll([clone(january), clone(february),
                                              clone(march), clone(april)], 'John Doe'))
      .then((_) => months.find({'days': 31}).excludeFields(['days', 'number', '_id']).data())
        .then((data){
          expect(data['data'], unorderedEquals([{'name': 'January'}, {'name': 'March'}]));
        });
    });

    test('exclude_nested', () {
      // when
      return ready.then((_) => months.addAll([{'a': {'b': 'c', 'd': 'e'}}], 'JD'))
      .then((_) => months.find().excludeFields(['_id', 'a.b']).data())
        .then((data){
          expect(data['data'], unorderedEquals([{'a': {'d': 'e'}}]));
        });
    });



    //temporarily, mongodb just ignores multiple ids

    skip_test('add data with same _id. (T03)', () {
// given
      Map january2 = {'name': 'January2', 'days': 11, 'number': 4, '_id': 'january'};
      Future shouldThrow = ready.then((_) => months.add(clone(january), 'John Doe'))

      // when
      .then((_) => months.add(clone(january2), 'John Doe'));

      // then
      expect(shouldThrow, throws);

    });

    test('addAll data with same _id. (T03.1)', () {
// given
      Map january2 = {'name': 'January2', 'days': 11, 'number': 4, '_id': 'january'};
      Future shouldThrow = ready.then((_) => months.addAll([clone(january)], 'John Doe'))

      // when
      .then((_) => months.addAll([clone(january2)], 'John Doe'));

      // then
      expect(shouldThrow, throws);

    });

    solo_test('breaking unique index constraint throws', () {
      // given

      var data = [];
      var toWait = [];
      return ready.then((_) =>
          months.change("2", {'a': 'a', 'b': 'b', '_id': "2"}, 'dummy', upsert: true))
       .then((_) =>
          months.change("1", {'a': 'a', 'b': 'b', '_id': "1"}, 'dummy', upsert: true))
      .then((_) =>
          months.change("1", {'a': 'a', 'b': 'bb', '_id': "1"}, 'dummy', upsert: true))
      .then((_) => months.data())
      .then((data){
        print(data);
      });
    });

    test('change data. (T04)', () {
      // given
      Map january2 = {'name': 'January2', 'days': 11, 'number': 4, '_id': 'january'};
      return ready.then((_) => months.add(clone(january), 'John Doe'))

      // when
        .then((_) => months.change('january', clone(january2), 'Michael Smith'))
        .then((_) => months.data())
        .then((data){

      // then
          expect(data['data'].length, equals(1));
          Map strippedData = data['data'][0];
          expect(strippedData, equals(january2));
          expect(data['version'], equals(2));
      }).then((_) => months.diffFromVersion(1))
        .then((dataDiff) {
          List diffList = dataDiff['diff'];
          expect(diffList.length, equals(1));
          Map diff = diffList[0];
          expect(diff['action'], equals('change'));
          expect(diff['_id'], equals('january'));
          Map strippedData = diff['data'];
          expect(strippedData, equals(january2));
          expect(diff['author'], equals('Michael Smith'));
        });
    });

    test('change data with jsonChange. (T04)', () {
      // given
      Map january2 = {'name': 'January2', 'days': 11, 'number': 4, '_id': 'january'};
      return ready.then((_) => months.add(clone(january), 'John Doe'))

      // when
        .then((_) => months.changeJson('january', [january, clone(january2)], 'Michael Smith'))
        .then((_) => months.data())
        .then((data){

      // then
          expect(data['data'].length, equals(1));
          Map strippedData = data['data'][0];
          expect(strippedData, equals(january2));
          expect(data['version'], equals(2));
      }).then((_) => months.diffFromVersion(1))
        .then((dataDiff) {
          List diffList = dataDiff['diff'];
          expect(diffList.length, equals(1));
          Map diff = diffList[0];
          expect(diff['action'], equals('change'));
          expect(diff['_id'], equals('january'));
          Map strippedData = diff['data'];
          expect(strippedData, equals(january2));
          expect(diff['author'], equals('Michael Smith'));
        });
    });

    test('change data with bad _id. (T06)', () {
      // given
      Future shouldThrow = ready.then((_) => months.add(clone(january), 'John Doe'))

      // when
        .then((_) => months.change('january', february, 'Michael Smith'));

      // then
        expect(shouldThrow, throws);
    });

    test('remove data. (T07)', () {
      // given
      Future toRemove = ready.then((_) => months.add(clone(january), 'John Doe'))

      // when
        .then((_) => months.remove('january', 'Michael Smith'));

      // then
      return toRemove.then((_) => months.data())
        .then((data) {
          expect(data['data'].length, equals(0));
          expect(data['version'], lessThanOrEqualTo(2));
        }).then((_) => months.diffFromVersion(1))
        .then((dataDiff) {
          List diffList = dataDiff['diff'];
          expect(diffList.length, equals(1));
          Map diff = diffList[0];
          expect(diff['action'], equals('remove'));
          expect(diff['_id'], equals('january'));
          expect(diff['version'], equals(2));
          expect(diff['author'], equals('Michael Smith'));
        });
    });

    test('removeAll data. (T07.1)', () {
      // given
      Future toRemove = ready.then((_) => months.addAll([january, february, march,
                                                         april], 'John Doe'))

      // when
        .then((_) => months.removeAll({'days': 31}, 'Michael Smith'));

      // then
      return toRemove.then((_) => months.data())
        .then((data) {
          expect(data['data'].length, equals(2));
          expect(data['version'], equals(4));
        }).then((_) => months.diffFromVersion(4))
        .then((dataDiff) {
          List diffList = dataDiff['diff'];
          expect(diffList.length, equals(2));
          Map diff = diffList[0];
          expect(diff['action'], equals('remove'));
          expect(diff['_id'], equals('january'));
          expect(diff['version'], equals(5));

          diff = diffList[1];
          expect(diff['action'], equals('remove'));
          expect(diff['_id'], equals('march'));
          expect(diff['version'], equals(6));
          expect(diff['author'], equals('Michael Smith'));
        });
    });

    test('remove nonexisting data. (T08)', () {
      // when
      Future shouldNotThrow = ready.then((_) => months.remove('january', 'Michael Smith'))
          .then(expectAsync1((res){
          }));
      // then
    });

    test('can reconstruct changes form diff. (T09)', () {
      // given
      Map january2 = {'name': 'January2', 'days': 11, 'number': 4, '_id': 'january'};
      Map march2 =  {'name': 'March2', 'days': 21, 'number': 14, '_id': 'march'};
      List dataStart;
      List dataEnd;

      Future multipleAccess =
          ready.then((_) => months.data()).then((data) => dataStart = data['data'] )

       .then((_) => months.addAll(clone([january, february, march, april]), 'John Doe'))
       .then((_) => months.change('january', january2, 'John Doe'))
       .then((_) => months.remove('february', 'John'))
       .then((_) => months.add(clone(february), 'John Doe'))
       .then((_) => months.remove('april', 'John'))
       .then((_) => months.add(clone(may), 'John Doe'))
       .then((_) => months.change('march', march2, 'John Doe'))
       .then((_) => months.change('january', january, 'John Doe'))
       .then((_) => months.data()).then((data) => dataEnd = data['data'] );
      //when
      return multipleAccess.then((_) => months.diffFromVersion(0))

      // then
      .then((dataDiff) {
         handleDiff(dataDiff['diff'], dataStart);
         expect(dataStart, unorderedEquals(dataEnd));
      });
    });

    test('deprecatedChange data. (T10)', () {
      // given
      Map january2 = {'name': 'January2', 'number': 4, '_id': 'january'};
      return ready.then((_) => months.add(clone(january), 'John Doe'))

      // when
        .then((_) => months.deprecatedChange('january', clone(january2), 'Michael Smith'))
        .then((_) => months.data())
        .then((data){

      // then
          expect(data['data'].length, equals(1));
          Map strippedData = data['data'][0];
          january2.forEach((key, value) {
            expect(strippedData[key], equals(value));
          });
          january.forEach((key, value) {
            if (!january2.containsKey(key)) expect(strippedData[key], equals(value));
          });
          expect(data['version'], equals(2));
      }).then((_) => months.diffFromVersion(1))
        .then((dataDiff) {
          List diffList = dataDiff['diff'];
          expect(diffList.length, equals(1));
          Map diff = diffList[0];
          expect(diff['action'], equals('change'));
          expect(diff['_id'], equals('january'));
          Map strippedData = diff['data'];
          var res = clone(january)..addAll(january2);
          expect(strippedData, equals(res));
          expect(diff['author'], equals('Michael Smith'));
        });
    });

    test('deprecatedChange not existing data. (T11)', () {
      // when
      Future shouldThrow =  ready.then((_) => months.deprecatedChange('january', clone(january), 'Michael Smith'));

      //then
        expect(shouldThrow, throws);
    });

    test('deprecatedChange data with bad _id. (T12)', () {
      // given
      Future shouldThrow = ready.then((_) => months.add(clone(january), 'John Doe'))

      // when
        .then((_) => months.deprecatedChange('january', clone(february), 'Michael Smith'));

      // then
        expect(shouldThrow, throws);
    });

    test('update data. (T13)', () {
      //when
      return ready
          .then((_) => months.addAll(clone(monthsCol), 'John Doe'))
          .then((_) => months.update({'days': 28},
            (Map document) {
              document["days"] = 29;
              document["number"] = 2;
              document["name"] = "February";

              return document;
            }, 'John Doe'))
          .then((_) => months.data())
          .then((dataInfo) {
            expect(dataInfo['version'], equals(13));
            var data = dataInfo['data'];
            expect(data[1], equals({'days': 29, 'number': 2, 'name': 'February', '_id': 'february'}));
            return dataInfo;
          })
          .then((_) => months.diffFromVersion(12))
          .then((dataDiff) {
            List diffList = dataDiff['diff'];
            expect(diffList.length, equals(1));
            Map diff = diffList[0];
            expect(diff['action'], equals('change'));
            expect(diff['_id'], equals('february'));
            Map strippedData = diff['data'];
            expect(strippedData, equals({'days': 29, 'number': 2, 'name': 'February', '_id': 'february'}));
            expect(diff['author'], equals('John Doe'));
          });
    });

    test('update data. (T13)', () {
      //when
      return ready
          .then((_) => months.addAll(clone(monthsCol), 'John Doe'))
          .then((_) => months.update({'days': 28},
            (Map document) {
              document["days"] = 29;
              document["number"] = 2;
              document["name"] = "February";

              return document;
            }, 'John Doe')).then((_) {
              return mongodb.rawDb.collection("__clean_months_history").find()
                .toList().then((data) {
                  Map oldData = data.where((m) => m['before']['name'] == 'February').first['before'];
                  expect(oldData['days'], equals(28));
              });
            });
    });

    test('update data with only one changed field. (T14)', () {
      //when
      return ready
          .then((_) => months.addAll(clone(monthsCol), 'John Doe'))
          .then((_) => months.update({'days': 31},
            (Map document) {
              document["number"] = 47;
              return document;
            }, 'John Doe'))
          .then((_) => months.data(stripVersion: false))
          .then((dataInfo) {
            expect(dataInfo['version'], equals(19));
            var data = dataInfo['data'];
            num version = 13;
            data.forEach((month) {
               if( month['days'] == 31) expect(month['number'], equals(47));
               if( month['days'] == 31) expect(month['__clean_version'], equals(version++));
            });
            return dataInfo;
          })
          .then((_) => months.diffFromVersion(12))
          .then((dataDiff) {
            num version = 13;
            dataDiff['diff'].forEach((elem) => expect(elem['version'], equals(version++)));
            List diffList = dataDiff['diff'];
            expect(diffList.length, equals(7));
          });
    });

    test('update data and remove one field. (T15)', () {
      //when
      return ready
          .then((_) => months.addAll(clone(monthsCol), 'John Doe'))
          .then((_) => months.update({'days': 31},
            (Map document) {
              document.remove("number");
              return document;

            }, 'John Doe'))
          .then((_) => months.data(stripVersion: false))
          .then((dataInfo) {
            expect(dataInfo['version'], equals(19));
            var data = dataInfo['data'];
            num version = 13;
            data.forEach((month) {
               if( month['days'] == 31) expect(month['number'], isNull);
               if( month['days'] == 31) expect(month['__clean_version'], equals(version++));
            });
            return dataInfo;
          })
          .then((_) => months.diffFromVersion(12))
          .then((dataDiff) {
            num version = 13;
            dataDiff['diff'].forEach((elem) => expect(elem['version'], equals(version++)));
            List diffList = dataDiff['diff'];
            expect(diffList.length, equals(7));
          });
    });

    test('stateless', (){
      return ready
          .then((_) => months.addAll(new List.from(monthsCol.map((e)=> clone(e))), 'John Doe'))
          .then((_){
            months.find({'days': 31}).limit(1).skip(1).fields(['name']);
            return months.find().data().then((data){
              expect(data['data'], unorderedEquals(monthsCol));
            });
          });
    });

    test('findOne with exactly one entry in db. (T16)', () {
      // when
      return ready.then((_) => months.add(clone(january), 'John Doe'))
      .then((_) => months.findOne())
      .then((data) {

        // then
        expect(data.length, equals(january.length));
        expect(data, equals(january));
      });

    });

    test('findOne with exactly zero entries in db. (T16.1)', () {
      // when
      return ready.then((_) => months.findOne())
      .then((_) => _)
      // then
      .catchError((error) {
        expect(error.message, equals("There are no entries in database."));
      });
    });

    test('findOne with more entries in db. (T16.2)', () {
      // when
      return ready.then((_) => months.addAll(
          [clone(january), clone(february), clone(march)], 'John Doe'))
      .then((_) => months.findOne())
      .then((_) => _)
      // then
      .catchError((error) {
        expect(error.message, equals("There are multiple entries in database."));
      });
    });

    test('cache should invalidate when changing the collection', () {
      var _mongodb = new MongoDatabase('mongodb://127.0.0.1/mongoProviderTest',
          cache: new Cache(new Duration(seconds: 1), 1000));
      ready = Future.wait(_mongodb.init)
                    .then((_) => _mongodb.dropCollection('months'))
                    .then((_) => _mongodb.removeLocks())
                    .then((_) => months = _mongodb.collection('months'))
                    ;
      return ready.then((_){
        months = _mongodb.collection('months');
        return months.add({'a': 'aa'}, '')
        .then((_) => months.data())
        .then((_) => months.add({'b': 'bb'}, ''))
        .then((_) => months.data())
        .then((data) => expect(data['data'].length, equals(2)))
        ;
      });

    });

  });
}
