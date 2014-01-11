// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library mongo_provider_test;

import "package:unittest/unittest.dart";
import "package:clean_sync/server.dart";
import "dart:async";
import 'dart:math';



void handleDiff(List<Map> diff, List collection) {
  diff.forEach((Map change) {
    if (change["action"] == "add") {
      collection.add(new Map.from(change["data"]));
    }
    else if (change["action"] == "change") {
      Map record = collection.firstWhere((d) => d["_id"] == change["_id"]);
      record.addAll(change["data"]);
    }
    else if (change["action"] == "remove") {
      Map record = collection.firstWhere((d) => d["_id"] == change["_id"]);
      collection.remove(record);
    }
  });
}

Map stripPrivateFields(Map<String, dynamic> data){
  Map newData = {};
  data.forEach((key,value) {
    if (!key.startsWith('__')) newData[key] = value;
  });
  return newData;
}

List stripPrivateFieldsList(List<Map<String, dynamic>> data){
  List newData = [];
  data.forEach((oldMap) {
    newData.add(stripPrivateFields(oldMap));
  });
  return newData;
}

void main() {
  group('MongoProvider', () {
    MongoProvider months;
    MongoProvider rndData;
    MongoProvider rndAllData;
    Future ready;
    MongoDatabase mongodb;
    Map january, february, march, april, may, june, july,
        august, september, october, november, december;
    List<Map> allMonths;
    Random rng;

     setUp(() {
      rng = new Random();
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
      allMonths = [january, february, march, april, may, june, july,
                   august, september, october, november, december];

      mongodb = new MongoDatabase('mongodb://0.0.0.0/mongoProviderTest');
      ready = Future.wait(mongodb.init).then((_) {
        mongodb.dropCollection('months');
        mongodb.dropCollection('random');
      })
      .then((_) => mongodb.randomChangeMongoProvider())
      .then((_){
        months = mongodb.collection('months');
        rndAllData = mongodb.collection('random');
//        rndData = mongodb.collection('random').find({'a.a': 'hello'});
        rndData = mongodb.collection('random').find({'a.a.a': {'\$gt': {}}});
      });
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
      return ready.then((_) => months.add(new Map.from(january), ''))
        .then((_) => months.data())
        .then((data){

      // then
          expect(data['data'].length, equals(1));
          Map strippedData = stripPrivateFields(data['data'][0]);
          expect(strippedData, equals(january));
          expect(data['version'], equals(1));
      }).then((_) => months.diffFromVersion(0))
        .then((dataDiff) {
          List diffList = dataDiff['diff'];
          expect(diffList.length, equals(1));
          Map diff = diffList[0];
          expect(diff['action'], equals('add'));
          expect(diff['_id'], equals('january'));
          Map strippedData = stripPrivateFields(diff['data']);
          expect(strippedData, equals(january));
          expect(diff['author'], equals(''));
        });
    });

    test('add data with same _id. (T03)', () {
// given
      Map january2 = {'name': 'January2', 'days': 11, 'number': 4, '_id': 'january'};
      Future shouldThrow = ready.then((_) => months.add(new Map.from(january), ''))

      // when
      .then((_) => months.add(new Map.from(january2), ''));

      // then
      expect(shouldThrow, throws);

    });

    test('change data. (T04)', () {
      // given
      Map january2 = {'name': 'January2', 'days': 11, 'number': 4, '_id': 'january'};
      return ready.then((_) => months.add(new Map.from(january), ''))

      // when
        .then((_) => months.change('january', new Map.from(january2), 'Michael Smith'))
        .then((_) => months.data())
        .then((data){

      // then
          expect(data['data'].length, equals(1));
          Map strippedData = stripPrivateFields(data['data'][0]);
          expect(strippedData, equals(january2));
          expect(data['version'], equals(2));
      }).then((_) => months.diffFromVersion(1))
        .then((dataDiff) {
          List diffList = dataDiff['diff'];
          expect(diffList.length, equals(1));
          Map diff = diffList[0];
          expect(diff['action'], equals('change'));
          expect(diff['_id'], equals('january'));
          Map strippedData = stripPrivateFields(diff['data']);
          expect(strippedData, equals(january2));
          expect(diff['author'], equals('Michael Smith'));
        });
    });

    test('change not existing data. (T05)', () {
      // when
      Future shouldThrow =  ready.then((_) => months.change('january', new Map.from(january), 'Michael Smith'));

      //then
        expect(shouldThrow, throws);
    });

    test('change data with bad _id. (T06)', () {
      // given
      Future shouldThrow = ready.then((_) => months.add(new Map.from(january), ''))

      // when
        .then((_) => months.change('january', new Map.from(february), 'Michael Smith'));

      // then
        expect(shouldThrow, throws);
    });

    test('remove data. (T07)', () {
      // given
      Future toRemove = ready.then((_) => months.add(new Map.from(january), ''))

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

    test('remove nonexisting data. (T08)', () {
      // when
      Future shouldNotThrow =ready.then((_) => months.remove('january', 'Michael Smith'));

      // then
        expect(shouldNotThrow, completion(isTrue));
    });

    test('can reconstruct changes form diff. (T09)', () {
      // given
      Map january2 = {'name': 'January2', 'days': 11, 'number': 4, '_id': 'january'};
      Map march2 =  {'name': 'March2', 'days': 21, 'number': 14, '_id': 'march'};
      List dataStart;
      List dataEnd;



      Future multipleAccess =
          ready.then((_) => months.data()).then((data) => dataStart = data['data'] )
          .then((_) => months.add(new Map.from(january), ''))
       .then((_) => months.add(new Map.from(february), ''))
       .then((_) => months.add(new Map.from(march), ''))
       .then((_) => months.add(new Map.from(april), ''))
         .then((_) => months.change('january', january2, ''))
       .then((_) => months.remove('february', ''))
       .then((_) => months.add(new Map.from(february), ''))
       .then((_) => months.remove('april', ''))
       .then((_) => months.add(new Map.from(may), ''))
       .then((_) => months.change('march', march2, ''))
       .then((_) => months.change('january', january, ''))
       .then((_) => months.data()).then((data) => dataEnd = data['data'] );
      //when
      return multipleAccess.then((_) => months.diffFromVersion(0))

      // then
      .then((dataDiff) {
         handleDiff(dataDiff['diff'], dataStart);

         expect(stripPrivateFieldsList(dataStart), equals(stripPrivateFieldsList(dataEnd)));
      });
    });

  });
}
