// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of server;

class DiffNotPossibleException implements Exception {
  final String cause;
  DiffNotPossibleException(this.cause);
}

class MongoProvider implements DataProvider {
  Db _db;
  Future _conn; // connection to database _db
  String _collectionName;
  Map _selector = {};

  Future<int> get _maxVersion => _collectionHistory.count();
  DbCollection get _collection => _db.collection(_collectionName);
  DbCollection get _collectionHistory =>
      _db.collection("${_collectionName}_history");

  MongoProvider(this._db, this._conn);

  MongoProvider collection(String collectionName) {
    var mp = new MongoProvider(_db, _conn);
    mp._collectionName = collectionName;
    return mp;
  }

  MongoProvider find(Map params) {
    var mp = new MongoProvider(_db, _conn);
      if(_collectionName != null) {
      mp = mp.collection(_collectionName);
    }
    mp._selector.addAll(this._selector);
    mp._selector.addAll(params);
    return mp;
  }

  Future<Map> data() {
    return Future.wait
        ([_conn.then((_) => _collection.find(_selector).toList()), _maxVersion])
        .then((results) => {'data': results[0], 'version': results[1]});
  }

  Future add(num _id, Map data, String author) {
    return Future.wait([_conn, _maxVersion]).then((results) {
      var nextVersion = results[1] + 1;
      return _collectionHistory.insert({
        "before" : {},
        "after" : data,
        "change" : {},
        "action" : "add",
        "author" : author,
        "version" : nextVersion
      }).then((_) {
        return _collection.insert(data);
      });
    });
  }

  Future change(num id, Map data, String author) {
    return Future.wait([_conn, _maxVersion]).then((results) {
      var nextVersion = results[1] + 1;
      return _collection.findOne({"_id" : id}).then((Map record) {
        Map newRecord = new Map.from(record);
        newRecord.addAll(data);

        return _collectionHistory.insert({
          "before" : record,
          "after" : newRecord,
          "change" : data,
          "action" : "change",
          "author" : author,
          "version" : nextVersion
        }).then((_) {
          return _collection.save(newRecord);
        });
      });
    });
  }

  Future remove(num id, String author) {
    return Future.wait([_conn, _maxVersion]).then((results) {
      var nextVersion = results[1] + 1;
      return _collection.findOne({"_id" : id}).then((Map record) {
        return _collectionHistory.insert({
          "before" : record,
          "after" : {},
          "change" : {},
          "action" : "remove",
          "author" : author,
          "version" : nextVersion
        }).then((_) {
          return _collection.remove({"_id" : record["_id"]});
        });
      });
    });
  }

  Future<Map> diffFromVersion(num version) {
    try{
      return _diffFromVersion(version).then((d) => {'diff': d});
    } on DiffNotPossibleException catch(e) {
      return data().then((d) {
        d['diff'] = null;
        return d;
      });
    }
  }

  Future<List<Map>> _diffFromVersion(num version) {
    // if (some case not covered so far) {
    // throw new DiffNotPossibleException('diff not possible');

    Map beforeSelector = {"\$query" : {"version" : {"\$gt" : version}, "before" : {"\$gt" : {}}}, "\$orderby" : {"version" : 1}};
    Map afterSelector = {"\$query" : {"version" : {"\$gt" : version}, "after" : {"\$gt" : {}}}, "\$orderby" : {"version" : 1}};

    _selector.forEach((k, v) {
      beforeSelector["\$query"]["before.${k}"] = v;
      afterSelector["\$query"]["after.${k}"] = v;
    });

    return _conn.then((_) {
      return Future.wait([
        _collectionHistory.find(beforeSelector).toList(),
        _collectionHistory.find(afterSelector).toList(),
      ]).then((responses) {
        List before = responses[0];
        List after = responses[1];
        List diff = [];

        int i = 0;
        int j = 0;

        while (i < before.length && j < after.length) {
          if (before[i]["version"] < after[j]["version"]) {
            diff.add({
              "action" : "remove",
              "_id" : before[i]["before"]["_id"],
              "version" : before[i]["version"],
              "author" : before[i]["author"],
              "collection" : _collectionName
            });

            i++;
          }
          else if (before[i]["version"] == after[j]["version"]) {
            diff.add({
              "action" : "change",
              "_id" : before[i]["before"]["_id"],
              "data" : before[i]["change"],
              "version" : before[i]["version"],
              "author" : before[i]["author"],
              "collection" : _collectionName
            });

            i++;
            j++;
          }
          else {
            diff.add({
              "action" : "add",
              "data" : after[j]["after"],
              "version" : after[j]["version"],
              "author" : after[j]["author"],
              "collection" : _collectionName
            });

            j++;
          }
        }

        while (i < before.length) {
          diff.add({
            "action" : "remove",
            "_id" : before[i]["before"]["_id"],
            "version" : before[i]["version"],
            "author" : before[i]["author"],
            "collection" : _collectionName
          });

          i++;
        }

        while (j < after.length) {
          diff.add({
            "action" : "add",
            "data" : after[j]["after"],
            "version" : after[j]["version"],
            "author" : after[j]["author"],
            "collection" : _collectionName
          });

          j++;
        }

        return diff;
      });
    });
  }
}
