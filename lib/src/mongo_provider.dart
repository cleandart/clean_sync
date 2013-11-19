// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.server;

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

const String VERSION_FIELD_NAME = '__clean_version';
const String LOCK_COLLECTION_NAME = '__clean_lock';
final Function historyCollectionName =
  (collectionName) => "__clean_${collectionName}_history";

class MongoDatabase {
  Db _db;
  Future _conn;
  List<Future> init = [];
  DbCollection _lock;

  MongoDatabase(String url) {
    _db = new Db(url);
    _conn = _db.open(); // open connection to database
    init.add(_conn);
    init.add(_conn.then((_) {
      _lock = _db.collection(LOCK_COLLECTION_NAME);
      return true;
      }));
  }

  void create_collection(String collectionName) {
    init.add(_conn.then((_) =>
        _db.createIndex(historyCollectionName(collectionName), key: 'version',
        unique: true)));
  }

  // TODO: keys can be also provided to mongo_dart createIndex function
  void createIndex(String collectionName, String key, {unique: false}){
    ["before", "after"].forEach((w) {
      init.add(_conn.then((_) =>
          _db.createIndex(historyCollectionName(collectionName),
          key: w + '.' + key, unique: unique)));
    });
    init.add(_conn.then((_) =>
        _db.createIndex(collectionName, key: key, unique: unique)));
  }

  MongoProvider collection(String collectionName) {
    DbCollection collection = _db.collection(collectionName);
    DbCollection collectionHistory =
        _db.collection(historyCollectionName(collectionName));
    var mp = new MongoProvider(collection, collectionHistory, _lock);
    return mp;
  }
}

class MongoProvider implements DataProvider {
  final DbCollection collection, _collectionHistory, _lock;
  List<Map> _selectorList = [];

  Future<int> get _maxVersion => _collectionHistory.count();

  MongoProvider(this.collection, this._collectionHistory, this._lock);

  MongoProvider find(Map params) {
    var mp = new MongoProvider(collection, _collectionHistory, _lock);
    mp._selectorList = new List.from(this._selectorList);
    mp._selectorList.add(params);
    return mp;
  }

  /**
   * Returns data and version of this data.
   */
  Future<Map> data() {
    Map selector = _selectorList.isEmpty ? {} : {AND: _selectorList};
    return collection.find(selector).toList().then((data) {
      var version = data.length == 0 ? 0 :
        data.map((item) => item['__clean_version']).reduce(max);
      return {'data': data, 'version': version};
    });
  }

  Future add(Map data, String author) {
    num nextVersion;
    return _get_locks().then((_) => _maxVersion).then((version) {
        nextVersion = version + 1;
        data[VERSION_FIELD_NAME] = nextVersion;
        return collection.insert(data);
      }).then((_) =>
        _collectionHistory.insert({
          "before" : {},
          "after" : data,
          "change" : {},
          "action" : "add",
          "author" : author,
          "version" : nextVersion
        })).then((_) => _release_locks());
  }

  Future change(Map data, String author) {
    num nextVersion;
    Map newRecord;
    return _get_locks().then((_) => collection.findOne({"_id" : data['_id']}))
      .then((Map record) {
        if(record == null) {
          return true;
        } else {
          return _maxVersion.then((version) {
            nextVersion = version + 1;
            newRecord = new Map.from(record);
            newRecord.addAll(data);
            newRecord[VERSION_FIELD_NAME] = nextVersion;
            return collection.save(newRecord);
          }).then((_) =>
            _collectionHistory.insert({
              "before" : record,
              "after" : newRecord,
              "change" : data,
              "action" : "change",
              "author" : author,
              "version" : nextVersion
            }));
        }
      }).then((_) => _release_locks());
  }

  Future remove(String _id, String author) {
    num nextVersion;
    return _get_locks().then((_) => _maxVersion).then((version) {
        nextVersion = version + 1;
        return collection.findOne({'_id': _id});
      }).then((record) {
        if (record == null) {
          return true;
        } else {
          return collection.remove({'_id': _id}).then((_) =>
            _collectionHistory.insert({
              "before" : record,
              "after" : {},
              "change" : {},
              "action" : "remove",
              "author" : author,
              "version" : nextVersion
          }));
        }
      }).then((_) => _release_locks());
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
      {QUERY : {"version" : {GT : version}, "before" : {GT: {}}},
       ORDERBY : {"version" : 1}};
    // selects records that fulfill _selector after change
    Map afterSelector =
      {QUERY : {"version" : {GT : version}, "after" : {GT: {}}},
       ORDERBY : {"version" : 1}};
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

  Future _get_locks() {
    print(collection.collectionName);
    return _lock.insert({'_id': collection.collectionName}).then(
      (_) => _lock.insert({'_id': _collectionHistory.collectionName}),
      onError: (e) {
        if(e['code'] == 11000) {
          // duplicate key error index
          return _get_locks();
        } else {
          throw(e);
        }
      }).then((_) => true);
  }

  Future _release_locks() {
    return _lock.remove({'_id': _collectionHistory.collectionName}).then((_) =>
    _lock.remove({'_id': collection.collectionName})).then((_) =>
    true);
  }
}
