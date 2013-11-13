// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of server;

class DiffNotPossibleException implements Exception {
   final String msg;
   const DiffNotPossibleException([this.msg]);
   String toString() => msg == null ? 'DiffNotPossible' : msg;
}


class MongoProvider implements DataProvider {
  final String QUERY = "\$query";
  final String GT = "\$gt";
  final String LT = "\$lt";
  final String ORDERBY = "\$orderby";
  final String OR = "\$or";

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

    // selects records that fulfilled _selector before change
    Map beforeSelector =
      {QUERY : {"version" : {GT : version}}, ORDERBY : {"version" : 1}};
    // selects records that fulfill _selector after change
    Map afterSelector =
      {QUERY : {"version" : {GT : version}}, ORDERBY : {"version" : 1}};
    // selects records that fulfill _selector before or after change
    Map beforeOrAfterSelector =
      {QUERY : {"version" : {GT : version}}, ORDERBY : {"version" : 1}};

    _selector.forEach((k, v) {
      beforeSelector[QUERY]["before.${k}"] = v;
      afterSelector[QUERY]["after.${k}"] = v;
      beforeOrAfterSelector[QUERY][OR] =
          [{"before.${k}" : v}, {"after.${k}" : v}];
    });

    List before, after, beforeOrAfter, diff;

    return _conn.then((_) =>
        _collectionHistory.find(beforeOrAfterSelector).toList())
      .then((result) {
        beforeOrAfter = result;
        return Future.wait([
          _collectionHistory.find(beforeSelector).toList(),
          _collectionHistory.find(afterSelector).toList()]);})
      .then((results) {
          before = results[0];
          after = results[1];
          diff = [];

          beforeOrAfter.forEach((record) {
            if(before.contains(record) && after.contains(record)) {
              // record was changed
              diff.add({
                "action" : "change",
                "_id" : record["before"]["_id"],
                "data" : record["change"],
                "version" : record["version"],
                "author" : record["author"],
                "collection" : _collectionName
              });
            } else if(before.contains(record)) {
              // record was removed
              diff.add({
                "action" : "remove",
                "_id" : record["before"]["_id"],
                "version" : record["version"],
                "author" : record["author"],
                "collection" : _collectionName
              });
            } else {
              // record was added
              diff.add({
                "action" : "add",
                "data" : record["after"],
                "version" : record["version"],
                "author" : record["author"],
                "collection" : _collectionName
              });
            }
          });

          return diff;
      });
  }
}
