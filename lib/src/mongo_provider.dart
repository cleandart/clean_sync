// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.server;

class DiffNotPossibleException implements Exception {
   final String msg;
   const DiffNotPossibleException([this.msg]);
   String toString() => msg == null ? 'DiffNotPossible' : msg;
}

class MongoException implements Exception {
   final Map mongoError;
   final String msg;
   const MongoException(this.mongoError, [this.msg]);
   String toString() =>
       msg == null ? 'MongoError: $mongoError' : '$msg MongoError: $mongoError';
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

  void close() {
    Future.wait(init).then((_) => _db.close());
  }

  void create_collection(String collectionName) {
    init.add(_conn.then((_) =>
        _db.createIndex(historyCollectionName(collectionName), key: 'version',
        unique: true)));
  }

  /**
   * Creates index on chosen collection and corresponding indexes on collection
   * history. keys is a map in form {field_name: 1 or -1} with 1/-1 specifying
   * ascending/descending order (same as the map passed to mongo function
   * ensureIndex).
   */
  void createIndex(String collectionName, Map keys, {unique: false}) {
    Map beforeKeys = {};
    Map afterKeys = {};
    keys.forEach((key, val) {
      beforeKeys['before.$key'] = val;
      afterKeys['after.$key'] = val;
    });
    beforeKeys['version'] = 1;
    afterKeys['version'] = 1;
    init.add(_conn.then((_) =>
        _db.createIndex(historyCollectionName(collectionName),
            keys: beforeKeys)));
    init.add(_conn.then((_) =>
        _db.createIndex(historyCollectionName(collectionName),
            keys: afterKeys)));
    init.add(_conn.then((_) =>
        _db.createIndex(collectionName, keys: keys, unique: unique)));
  }

  MongoProvider collection(String collectionName) {
    DbCollection collection = _db.collection(collectionName);
    DbCollection collectionHistory =
        _db.collection(historyCollectionName(collectionName));
    return new MongoProvider(collection, collectionHistory, _lock);
  }

  Future dropCollection(String collectionName) =>
    Future.wait([
      _db.collection(collectionName).drop(),
      _db.collection(historyCollectionName(collectionName)).drop()
    ]);

  Future removeLocks() => _lock.drop();
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
        }),
      onError: (e) {
        // Errors thrown by MongoDatabase are Map objects with fields err, code,
        // ...
        return _release_locks().then((_) {
          throw new MongoException(e);
        });
      }
      ).then((_) => _release_locks());
  }

  Future change(String _id, Map change, String author) {
    num nextVersion;
    Map newRecord;
    return _get_locks().then((_) => collection.findOne({"_id" : _id}))
      .then((Map record) {
        if(record == null) {
          throw new MongoException(null,
              'Change was not applied, document with id $_id does not exist.');
        } else if (change.containsKey('_id') && change['_id'] != _id) {
          throw new MongoException(null,
              'New document id ${change['_id']} should be same as old one $_id.');
        } else {
          return _maxVersion.then((version) {
            nextVersion = version + 1;
            newRecord = new Map.from(record);
            newRecord.addAll(change);
            newRecord[VERSION_FIELD_NAME] = nextVersion;
            return collection.save(newRecord);
          }).then((_) =>
            _collectionHistory.insert({
              "before" : record,
              "after" : newRecord,
              "change" : change,
              "action" : "change",
              "author" : author,
              "version" : nextVersion
            }));
        }
      },
      onError: (e) {
        // Errors thrown by MongoDatabase are Map objects with fields err, code,
        // ...
        return _release_locks().then((_) {
          throw new MongoException(e);
        });
      }
      ).then((_) => _release_locks());
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
      },
      onError: (e) {
        // Errors thrown by MongoDatabase are Map objects with fields err, code,
        // ...
        return _release_locks().then((_) {
          throw new MongoException(e);
        });
      }
      ).then((_) => _release_locks());
  }

  Future<Map> diffFromVersion(num version) {
    try{
      return _diffFromVersion(version).then((d) {
      return {'diff': d};
      });
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
    Map beforeSelector = {QUERY : {}, ORDERBY : {"version" : 1}};
    // selects records that fulfill _selector after change
    Map afterSelector = {QUERY : {}, ORDERBY : {"version" : 1}};
    // selects records that fulfill _selector before or after change
    Map beforeOrAfterSelector = {QUERY : {}, ORDERBY : {"version" : 1}};

    // {before: {GT: {}}} to handle selectors like {before.age: null}
    List<Map> _beforeSelector = [{"version" : {GT : version}}, {"before" : {GT: {}}}];
    List<Map> _afterSelector = [{"version" : {GT : version}}, {"after" : {GT: {}}}];
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
