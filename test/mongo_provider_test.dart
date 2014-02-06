// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library mongo_provider_test;

import "package:unittest/unittest.dart";
import "package:clean_sync/server.dart";
import "dart:async";



void handleDiff(List<Map> diff, List collection) {
  print(diff);
  diff.forEach((Map change) {
    if (change["action"] == "add") {
      collection.add(new Map.from(change["data"]));
    }
    else if (change["action"] == "change") {
      Map record = collection.firstWhere((d) => d["_id"] == change["_id"]);
      collection.remove(record);
      collection.add(change["data"]);
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

     setUp(() {
      january = {'name': 'January', 'days': 31, 'number': 1, '_id': 'january'};
      february = {'name': 'February', 'days': 28, 'number': 2, '_id': 'february'};
      march =  {'name': 'March', 'days': 31, 'number': 3, '_id': 'march'};
      april = {'name': 'April', 'days': 30, 'number': 4, '_id': 'april'};
      may = {'name': 'May', 'days': 31, 'number': 5, '_id': 'may'};
      june = {'name': 'June', 'days': 30, 'number': 6, '_id': 'may'};
      july = {'name': 'July', 'days': 31, 'number': 7, '_id': 'july'};
      august = {'name': 'August', 'days': 31, 'number': 8, '_id': 'august'};
      september = {'name': 'September', 'days': 30, 'number': 9, '_id': 'september'};
      october = {'name': 'October', 'days': 31, 'number': 10, '_id': 'october'};
      november = {'name': 'November', 'days': 30, 'number': 11, '_id': 'november'};
      december = {'name': 'December', 'days': 31, 'number': 12, '_id': 'december'};

      mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest');
      ready = Future.wait(mongodb.init).then((_) => mongodb.dropCollection('months'))
                    .then((_) => mongodb.removeLocks())
                    .then((_) => months = mongodb.collection('months'));
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
      return ready.then((_) => months.add(new Map.from(january), 'John Doe'))
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
      return ready.then((_) => months.addAll([new Map.from(january), new Map.from(february)], 'John Doe'))
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

    test('add data with same _id. (T03)', () {
// given
      Map january2 = {'name': 'January2', 'days': 11, 'number': 4, '_id': 'january'};
      Future shouldThrow = ready.then((_) => months.add(new Map.from(january), 'John Doe'))

      // when
      .then((_) => months.add(new Map.from(january2), 'John Doe'));

      // then
      expect(shouldThrow, throws);

    });

    test('addAll data with same _id. (T03.1)', () {
// given
      Map january2 = {'name': 'January2', 'days': 11, 'number': 4, '_id': 'january'};
      Future shouldThrow = ready.then((_) => months.addAll([new Map.from(january)], 'John Doe'))

      // when
      .then((_) => months.addAll([new Map.from(january2)], 'John Doe'));

      // then
      expect(shouldThrow, throws);

    });

    test('change data. (T04)', () {
      // given
      Map january2 = {'name': 'January2', 'days': 11, 'number': 4, '_id': 'january'};
      return ready.then((_) => months.add(new Map.from(january), 'John Doe'))

      // when
        .then((_) => months.change('january', new Map.from(january2), 'Michael Smith'))
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
      Future shouldThrow = ready.then((_) => months.add(new Map.from(january), 'John Doe'))

      // when
        .then((_) => months.change('january', new Map.from(february), 'Michael Smith'));

      // then
        expect(shouldThrow, throws);
    });

    test('remove data. (T07)', () {
      // given
      Future toRemove = ready.then((_) => months.add(new Map.from(january), 'John Doe'))

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
            expect(res is num, isTrue);
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

       .then((_) => months.add(new Map.from(january), 'John Doe'))
       .then((_) => months.add(new Map.from(february), 'John Doe'))
       .then((_) => months.add(new Map.from(march), 'John Doe'))
       .then((_) => months.add(new Map.from(april), 'John Doe'))
       .then((_) => months.change('january', january2, 'John Doe'))
       .then((_) => months.remove('february', 'John'))
       .then((_) => months.add(new Map.from(february), 'John Doe'))
       .then((_) => months.remove('april', 'John'))
       .then((_) => months.add(new Map.from(may), 'John Doe'))
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
      return ready.then((_) => months.add(new Map.from(january), 'John Doe'))

      // when
        .then((_) => months.deprecatedChange('january', new Map.from(january2), 'Michael Smith'))
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
          var res = new Map.from(january)..addAll(january2);
          expect(strippedData, equals(res));
          expect(diff['author'], equals('Michael Smith'));
        });
    });

    test('deprecatedChange not existing data. (T11)', () {
      // when
      Future shouldThrow =  ready.then((_) => months.deprecatedChange('january', new Map.from(january), 'Michael Smith'));

      //then
        expect(shouldThrow, throws);
    });

    test('deprecatedChange data with bad _id. (T12)', () {
      // given
      Future shouldThrow = ready.then((_) => months.add(new Map.from(january), 'John Doe'))

      // when
        .then((_) => months.deprecatedChange('january', new Map.from(february), 'Michael Smith'));

      // then
        expect(shouldThrow, throws);
    });

  });
}
