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

Map _stripPrivateFields(Map<String, dynamic> data){
  Map newData = {};
  data.forEach((key,value) {
    if (!key.startsWith('__')) newData[key] = value;
  });
  return newData;
}

List _stripPrivateFieldsList(List<Map<String, dynamic>> data){
  List newData = [];
  data.forEach((oldMap) {
    newData.add(_stripPrivateFields(oldMap));
  });
  return newData;
}

void main() {
  group('MongoProvider', () {
    MongoProvider months;
    MongoProvider rndData;
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
        rndData = mongodb.collection('random');
//        rndData = mongodb.collection('random').find({'a.a': 'hello');
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
          Map strippedData = _stripPrivateFields(data['data'][0]);
          expect(strippedData, equals(january));
          expect(data['version'], equals(1));
      }).then((_) => months.diffFromVersion(0))
        .then((dataDiff) {
          List diffList = dataDiff['diff'];
          expect(diffList.length, equals(1));
          Map diff = diffList[0];
          expect(diff['action'], equals('add'));
          expect(diff['_id'], equals('january'));
          Map strippedData = _stripPrivateFields(diff['data']);
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
          Map strippedData = _stripPrivateFields(data['data'][0]);
          expect(strippedData, equals(january2));
          expect(data['version'], equals(2));
      }).then((_) => months.diffFromVersion(1))
        .then((dataDiff) {
          List diffList = dataDiff['diff'];
          expect(diffList.length, equals(1));
          Map diff = diffList[0];
          expect(diff['action'], equals('change'));
          expect(diff['_id'], equals('january'));
          Map strippedData = _stripPrivateFields(diff['data']);
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

         expect(_stripPrivateFieldsList(dataStart), equals(_stripPrivateFieldsList(dataEnd)));
      });
    });

    randomChoice(Iterable iter){
      var list = new List.from(iter);
      return list[rng.nextInt(list.length)];
    }

    var allData=['hello', 'world', 1, null];
    var allKeys=['a','b','c'];

    randomChangeMap(Map data){
      var key = randomChoice(allKeys);
      if (data.containsKey(key)){
        if (data[key] is Map){
          randomChangeMap(data[key]);
        } else {
          data[key] = randomChoice(allData);
        }
      } else {
        data[key] = randomChoice(allData);
      }

      if (data[key] is! Map && rng.nextInt(4) == 0) {
        data[key] = new Map();
        randomChangeMap(data[key]);
      }

      if (rng.nextInt(5) == 0) {
        data.remove(key);
      }
    }

    Future fetchAllCollections(List<MongoProvider> colls, Map res){
      Future.forEach(colls, (MongoProvider coll) {
        coll.data()
          .then((data){
            res[coll] = data;
          });
      });
    }

    Future makeRandomChange(MongoProvider coll, Set ids){
      String id = rng.nextInt(4).toString();
      num change = rng.nextInt(50);
      if (change <= 10) {
        // add
        if (ids.contains(id)) {
          return new Future.value();
        } else {
          ids.add(id);
          return coll.add({'_id': id}, '');
        }
      }
      else if (change <= 11) {
        // remove
        if (!ids.contains(id)) {
          return new Future.value();
        } else {
          ids.remove(id);
          return coll.remove(id, '');
        }
      } else {
        // change
        if (!ids.contains(id)) {
          return new Future.value();
        } else {
          return coll.find({'_id':id}).data().then((datas){
            var _data = datas['data'][0];
            randomChangeMap(_data);
            return coll.change(id, _data, '');
          });
        }
      }
    }

    toStringOrdered(List<Map> data){
      compare(a, b){
        return a['_id'].compareTo(b['_id']);
      }
      mapToStringOrdered(Map m, StringBuffer sb){
        sb.write('{');
        List toWrite = new List.from(m.keys)..sort();
        for (var key in toWrite) {
          sb.write('${key}: ');
          if(m[key] is! Map){
            sb.write(m[key].toString());
          } else {
            mapToStringOrdered(m[key], sb);
          }
          if (key != toWrite.last) sb.write(', ');
        }
        sb.write('}');
      }

      data.sort(compare);
      StringBuffer sb = new StringBuffer('[');
      for(var d in data){
        mapToStringOrdered(d, sb);
        sb.write(', ');
      }
      sb.write(']');
      return sb.toString();
    }

    solo_test('can reconstruct changes form diff. (T09)', () {
      // given
      List dataStart;
      List dataEnd;
      Set ids;
      ids = new Set();
      var versionEnd = 0;

      return ready.then( (_) => Future.forEach(new List.filled(10, 0) , (_) {
        return rndData.data().then(
          (data) {
            dataStart = data['data'];
            dataStart.forEach((e)=> ids.add(e['_id']));
            return Future.forEach(new List.filled(100, 0), (_) =>
                makeRandomChange(rndData, ids));

          }).then((_) => rndData.data())
            .then((data){ dataEnd = data['data']; })
            .then((_) => rndData.diffFromVersion(versionEnd))
            .then((dataDiff) {
              (dataDiff['diff'] as List).forEach((e) => versionEnd = max(versionEnd, e['version']));
              handleDiff(dataDiff['diff'], dataStart);
              print(toStringOrdered(dataEnd));
              expect(toStringOrdered(_stripPrivateFieldsList(dataStart)),
                    equals(toStringOrdered(_stripPrivateFieldsList(dataEnd))));
            });
      }));
    });
  });
}
