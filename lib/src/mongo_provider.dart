// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of server;

class DiffNotPossibleException implements Exception {
   final String msg;
   const DiffNotPossibleException([this.msg]);
   String toString() => msg == null ? 'DiffNotPossible' : msg;
}

const String QUERY = "\$query";
const String GT = "\$gt";
const String LT = "\$lt";
const String ORDERBY = "\$orderby";
const String OR = "\$or";
const String AND = "\$and";

class MongoDatabase {
  Db _db;
  MongoDatabase(this._db);

  MongoProvider collection(String collectionName) {
    DbCollection collection = _db.collection(collectionName);
    DbCollection collectionHistory =
        _db.collection("${collectionName}_history");
    var mp = new MongoProvider(collection, collectionHistory);
    return mp;
  }

  void ensureIndex(String collectionName){

  }
}

class MongoProvider implements DataProvider {
  DbCollection _collection, _collectionHistory;
  List<Map> _selectorList = [];

  Future<int> get _maxVersion => _collectionHistory.count();

  MongoProvider(this._collection, this._collectionHistory);

  MongoProvider find(Map params) {
    var mp = new MongoProvider(_collection, _collectionHistory);
    mp._selectorList = new List.from(this._selectorList);
    mp._selectorList.add(params);
    return mp;
  }

  /**
   * Returns data and version of this data. The following approach is used to
   * ensure consistency: 1. obtain version; 2. obtain data; 3. obtain version
   * again, if it differs from previously obtained version, go to step 2,
   * otherwise return data and version.
   */
  Future<Map> data() {
    Map selector = _selectorList.isEmpty ? {} : {AND: _selectorList};
    List data;
    int version;
    Function getDataAndVersion;
    getDataAndVersion = (_) {
      return _collection.find(selector).toList().then((d) {
        data = d;
        return _maxVersion;
      }).then((int v) {
        if(v == version){
          return {'data': data, 'version': version};
        } else {
          version = v;
          return getDataAndVersion();
        }
      });
    };
    return _maxVersion.then((v) {version=v;}).then(getDataAndVersion);
  }

  Future add(num _id, Map data, String author) {
    return _maxVersion.then((version) {
      var nextVersion = version + 1;
      return _collectionHistory.insert({
        "before" : {},
        "after" : data,
        "change" : {},
        "action" : "add",
        "author" : author,
        "version" : nextVersion
      }).then((_) {
        return _collection.insert(data);
      },
      onError: (e) {
        if(e['code'] == 11000) {
          // duplicate key error index
          return add(_id, data, author);
        } else {
          throw(e);
        }
      });
    });
  }

  Future change(num _id, Map data, String author) {
    return _maxVersion.then((version) {
      var nextVersion = version + 1;
      return _collection.findOne({"_id" : _id}).then((Map record) {
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
        },
        onError: (e) {
          if(e['code'] == 11000) {
            // duplicate key error index
            return change(_id, data, author);
          } else {
            throw(e);
          }
        });
      });
    });
  }

  Future remove(num _id, String author) {
    return _maxVersion.then((version) {
      var nextVersion = version + 1;
      return _collection.findOne({"_id" : _id}).then((Map record) {
        return _collectionHistory.insert({
          "before" : record,
          "after" : {},
          "change" : {},
          "action" : "remove",
          "author" : author,
          "version" : nextVersion
        }).then((_) {
          return _collection.remove({"_id" : record["_id"]});
        },
        onError: (e) {
          if(e['code'] == 11000) {
            // duplicate key error index
            return remove(_id, author);
          } else {
            throw(e);
          }
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

    if(!_selectorList.isEmpty){
      List<Map> _beforeSelector = [];
      List<Map> _afterSelector = [];
      _selectorList.forEach((item) {
        Map itemB = {};
        Map itemA = {};
        item.forEach((key, val) {
          itemB["before.${key}"] = val;
          itemA["after.${key}"] = val;
        });
        _beforeSelector.add(itemB);
        _afterSelector.add(itemA);
      });
      beforeSelector[QUERY][AND] = _beforeSelector;
      afterSelector[QUERY][AND] = _afterSelector;
      beforeOrAfterSelector[QUERY][OR] = [{AND: _beforeSelector},
                                          {AND: _afterSelector}];
    }

    Set before, after;
    List beforeOrAfter, diff;

    return _collectionHistory.find(beforeOrAfterSelector).toList()
      .then((result) {
        beforeOrAfter = result;
        return Future.wait([
          _collectionHistory.find(beforeSelector).toList(),
          _collectionHistory.find(afterSelector).toList()]);})
      .then((results) {
          before = new Set.from(results[0].map((d) => d['_id']));
          after = new Set.from(results[1].map((d) => d['_id']));
          diff = [];

          beforeOrAfter.forEach((record) {
            if(before.contains(record['_id']) && after.contains(record['_id']))
            {
              // record was changed
              diff.add({
                "action" : "change",
                "_id" : record["before"]["_id"],
                "data" : record["change"],
                "version" : record["version"],
                "author" : record["author"],
              });
            } else if(before.contains(record['_id'])) {
              // record was removed
              diff.add({
                "action" : "remove",
                "_id" : record["before"]["_id"],
                "version" : record["version"],
                "author" : record["author"],
              });
            } else {
              // record was added
              diff.add({
                "action" : "add",
                "_id" : record["after"]["_id"],
                "data" : record["after"],
                "version" : record["version"],
                "author" : record["author"],
              });
            }
          });
          return diff;
      });
  }
}
