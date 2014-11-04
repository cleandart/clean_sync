// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.server;

class ModifierException implements Exception {
  final error;
  final stackTrace;
  ModifierException(this.error, this.stackTrace);
  String toString() => "Modifier Error: $error \n Stack trace: $stackTrace";
}

class DiffNotPossibleException implements Exception {
   final String msg;
   const DiffNotPossibleException([this.msg]);
   String toString() => msg == null ? 'DiffNotPossible' : msg;
}

class MongoException implements Exception {
   final mongoError;
   final String msg;
   final StackTrace stackTrace;
   const MongoException(this.mongoError, this.stackTrace, [this.msg]);
   String toString() =>
       msg == null ?
           'MongoError: $mongoError\nStack trace: $stackTrace' :
           '$msg\nMongoError: $mongoError\nStack trace: $stackTrace';
}

class BreakException {
  final val;
  BreakException([this.val = null]);
}

const String QUERY = "\$query";
const String GT = "\$gt";
const String GTE = "\$gte";
const String LT = "\$lt";
const String LTE = "\$lte";
const String ORDERBY = "\$orderby";
const String OR = "\$or";
const String AND = "\$and";
const String SET = "\$set";
const String UNSET = "\$unset";
const String PUSH = "\$push";
const String NE = '\$ne';
const String IN = '\$in';

const num ASC = 1;
const num DESC = -1;
const num NOLIMIT = 0;
const num NOSKIP = 0;

const String VERSION_FIELD_NAME = '__clean_version';
const String LOCK_COLLECTION_NAME = '__clean_lock';
const String COLLECTION_NAME = '__clean_collection';

final Function historyCollectionName =
  (collectionName) => "__clean_${collectionName}_history";


class MongoConnection {
  Db _db;
  LockRequestor _lockRequestor;
  Cache cache;
  bool _initialized = false;

  static const _dbLock = "dblock";

  Db get rawDb => _db;

  MongoConnection(String url, LockRequestor this._lockRequestor, {Cache this.cache: dummyCache}) {
    _db = new Db(url);
  }

  static Future<MongoConnection> noLocking(String url, {Cache cache: dummyCache, int port: 27005}) {
    Locker locker;
    return
        Locker.bind("127.0.0.1", port)
          .then((Locker l) => locker = l)
          .then((_) => LockRequestor.connect("127.0.0.1", port))
          .then((LockRequestor lockRequestor) {
            lockRequestor.done.then((_) => locker.close());
            return new MongoConnection(url, lockRequestor, cache: cache);
        });
  }

  Future init() {
    if (_initialized) {
      throw new Exception('MongoConnection.init was already called.');
    }
    _initialized = true;
    return _db.open();
  }

  Future transact(callback(MongoDatabase _)) {
    Map meta = _lockRequestor.getZoneMetaData();
    MongoDatabase mdb;
    // Dispose only if this is the root transact
    bool shouldDispose = false;
    if (meta == null) {
      // Not in Zone yet => this is the root transact => create MongoDatabase
      mdb = new MongoDatabase(_db, this, cache: cache);
      shouldDispose = true;
    } else {
      mdb = meta['db'];
    }
    return _lockRequestor.withLock(_dbLock,
        () => new Future.sync(() => callback(mdb))
          .whenComplete(() => shouldDispose ? mdb.dispose() : null)
      , metaData: {'db' : mdb});
  }

  MongoProvider collection(String collectionName) {
    DbCollection collection = _db.collection(collectionName);
    DbCollection collectionHistory =
        _db.collection(historyCollectionName(collectionName));
    return new MongoProvider(this, collection, collectionHistory, cache);
  }

  Future close() => _db.close();

}

class MongoDatabase {
  Db _db;
  Cache cache;
  MongoConnection connection;
  Db get rawDb => _db;
  List<Future> operations = [];
  bool _disposed = false;

  MongoDatabase(Db this._db, MongoConnection this.connection, {Cache this.cache: dummyCache} );

  Future dispose() => Future.wait(operations).then((_) => _disposed = true);

  Future _logOperation(Future op()) {
    if (_disposed) throw new Exception("MongoDatabase is already disposed, no operations should be executed");
    Completer completer = new Completer();
    operations.add(completer.future);
    return op().whenComplete(() => completer.complete());
  }

  Future create_collection(String collectionName) {
    var histColName = historyCollectionName(collectionName);
    return _logOperation(
        () => _db.createIndex(histColName, key: 'version', unique: true)
        .then((_) => _db.createIndex(histColName, key: 'clientVersion', unique: true, sparse: true))
        .then((_) => _db.createIndex(histColName, keys: {'before._id': 1, 'version': 1}, unique: true))
        .then((_) => _db.createIndex(histColName, keys: {'after._id': 1, 'version': 1}, unique: true))
    );
  }

  /**
   * Creates index on chosen collection and corresponding indexes on collection
   * history. keys is a map in form {field_name: 1 or -1} with 1/-1 specifying
   * ascending/descending order (same as the map passed to mongo function
   * ensureIndex).
   */
  Future createIndex(String collectionName, Map keys, {unique: false,
    sparse: false}) {
    if (keys.isEmpty) return _logOperation(() => new Future.value(null));
    Map beforeKeys = {};
    Map afterKeys = {};
    keys.forEach((key, val) {
      beforeKeys['before.$key'] = val;
      afterKeys['after.$key'] = val;
    });
    beforeKeys['version'] = 1;
    afterKeys['version'] = 1;
    return _logOperation(() => _db.createIndex(historyCollectionName(collectionName),
            keys: beforeKeys)
        .then((_) => _db.createIndex(historyCollectionName(collectionName),
            keys: afterKeys))
        .then((_) => _db.createIndex(collectionName, keys: keys, unique: unique,
            sparse: sparse)));
  }

  MongoProvider collection(String collectionName) => connection.collection(collectionName);

  Future dropCollection(String collectionName) =>
      _logOperation(() => Future.wait([
      _db.collection(collectionName).drop(),
      _db.collection(historyCollectionName(collectionName)).drop()
    ]));

   Future _operation(callback()) =>
       _logOperation(() => new Future.sync(callback));

}

List addFieldIfNotEmpty(List fields, String field){
  if (fields.isNotEmpty) {
    var res = new List.from(fields)..add(field);
    return res;
  } else {
    return fields;
  }
}

MongoProvider mpClone(MongoProvider source){

  MongoProvider m = new MongoProvider.config(source.mongoConn, source.collection,
      source._collectionHistory, source.cache, source.idgen);
  m._selectorList = new List.from(source._selectorList);
  m._sortParams = new Map.from(source._sortParams);
  m._limit = source._limit;
  m._skip = source._skip;
  m._fields = new List.from(source._fields);
  m._excludeFields = new List.from(source._excludeFields);
  return m;
}

class MongoProvider implements DataProvider {
  final DbCollection collection, _collectionHistory;
  MongoConnection mongoConn;
  List<Map> _selectorList = [];
  Map _sortParams = {};
  List _excludeFields = [];
  List _fields = [];
  num _limit = NOLIMIT;
  num _skip = NOSKIP;
  Cache cache;
  IdGenerator idgen;

  //for testing purposes
  Future<int> get maxVersion => _maxVersion;

  Future<int> get _maxVersion =>
      _collectionHistory.find(where.sortBy('version', descending : true)
          .limit(1)).toList()
      .then((data) => data.isEmpty? 0: data.first['version']);

  Map get _rawSelector => {QUERY: _selectorList.isEmpty ?
      {} : {AND: _selectorList}, ORDERBY: _sortParams};

  MongoProvider(mongodb, collection, collectionHistory, cache) :
    this.config(mongodb, collection, collectionHistory, cache,
        new IdGenerator(getIdPrefix()));

  MongoProvider.config(this.mongoConn, this.collection, this._collectionHistory,
      this.cache, this.idgen);

  Future deleteHistory(num version) {
    return _collectionHistory.remove({'version': {LT: version}});
  }

  MongoProvider fields(List<String> fields) {
    var res = mpClone(this);
    res._fields.addAll(fields);
    return res;
  }

  MongoProvider excludeFields(List<String> excludeFields) {
    var res = mpClone(this);
    res._excludeFields.addAll(excludeFields);
    return res;
  }

  MongoProvider find([Map params = const {}]) {
    var res = mpClone(this);
    res._selectorList.add(params);
    return res;
  }

  MongoProvider sort(Map params) {
    var res = mpClone(this);
    res._sortParams.addAll(params);
    return res;
  }

  MongoProvider limit(num value) {
    var res = mpClone(this);
    res._limit = value;
    return res;
  }

  MongoProvider skip(num value) {
    var res = mpClone(this);
    res._skip = value;
    return res;
  }

//  dynamic repr(String operation) {
//    return Tpl();
//  }

  String get repr{
    return '${collection.collectionName}$_selectorList$_sortParams$_limit$_skip$_fields$_excludeFields';
  }

  /**
   * Returns key-value pairs according to the specified selectors.
   * There should be exactly one entry with specified selectors, otherwise
   * findOne throws an [Exception].
   */
  Future<Map> findOne() {
    return data().then((Map result) {
      List data = result["data"];

      if (data.isEmpty) {
        throw new Exception("There are no entries in database.");
      } else if (data.length > 1) {
        throw new Exception("There are multiple entries in database.");
      }

      return new Future.value(data[0]);
    });
  }

  Future<Map> data({stripVersion: true}) {
    return cache.putIfAbsent('data $repr', () => _data(stripVersion: stripVersion));
  }

  Future<DataSet> getDataSet() {
    return data().then((data) => new DataSet.from(data['data'])..addIndex(['_id']));
  }

  createSelector(Map selector, List fields, List excludeFields) {
    var sel = new SelectorBuilder().raw(selector);
    if (fields.isNotEmpty) {
      sel.fields(fields);
    }
    if (excludeFields.isNotEmpty) {
      sel.excludeFields(excludeFields);
    }
    return sel;
  }

  Future<bool> _clientVersionExists(String clientVersion) =>
      _collectionHistory.find(where.eq('clientVersion', clientVersion).limit(1)).toList()
      .then((data) => !data.isEmpty);

  _checkClientVersion(String clientVersion) {
    if (clientVersion != null) {
      return _clientVersionExists(clientVersion).then((exists) {
        if (exists) throw new BreakException();
      });
    }
  }

  _checkInferredAction(action, inferredAction, upsert){
    if (action != inferredAction) {
      if (!(action == 'change' &&
            inferredAction == 'add' &&
            upsert == true))
        throw new BreakException();
    }
  }


  _processError(var e, [var s]) {
    if (e is BreakException) {
      return e.val;
    } else {
      logger.shout("MP _processError", e, s);
      throw e;
    }
  }

  get _selector {
    var __fields = addFieldIfNotEmpty(_fields, VERSION_FIELD_NAME);
    return createSelector(_rawSelector, __fields, _excludeFields)
                                   .limit(_limit).skip(_skip);
  }


  /**
   * Returns data and version of this data.
   */
  Future<Map> _data({stripVersion: true}) {
    return collection.find(_selector).toList().then((data) {
      num watchID = startWatch('MP data ${collection.collectionName}');
      var version = data.length == 0 ? 0 : data.map((item) => item['__clean_version']).reduce(max);
      if(stripVersion) _stripCleanVersion(data);
      assert(version != null);
      // Add collection name to document (it's not in the database!)
      data.forEach((e) => e[COLLECTION_NAME] = collection.collectionName);
      return {'data': data, 'version': version};
    }).then((result) {
      stopWatch(watchID);
      return result;
    });
  }


  Future addAll(List<Map> data, String author) {
    for (Map d in data) ensureId(d, idgen);
    cache.invalidate();
    num nextVersion;
    return mongoConn.transact((MongoDatabase mdb) =>
      mdb._operation(() => _maxVersion.then((version) {
        nextVersion = version + 1;
        data.forEach((elem) => elem[VERSION_FIELD_NAME] = nextVersion++);
        return collection.insertAll(data);
      }).then((_) =>
        _collectionHistory.insertAll(data.map((elem) =>
            {
              "before" : {},
              "after" : stripCollectionName(elem),
              "action" : "add",
              "author" : author,
              "version" : elem[VERSION_FIELD_NAME],
              "timestamp" : new DateTime.now(),
            }).toList(growable: false)),
      onError: (e,s) {
        // Errors thrown by MongoDatabase are Map objects with fields err, code,
        logger.warning('MP update error:', e, s);
          throw new MongoException(e,s);
      }
      ))).then((_) => nextVersion);
  }


  Future writeOperation(String _id, String author, String action, Map newData,
                        {String clientVersion: null, upsert: false}) {
    cache.invalidate();
    num nextVersion;
    return mongoConn.transact((MongoDatabase mdb) =>
      mdb._operation(() => new Future.sync(() => _checkClientVersion(clientVersion))
      .then((_) => collection.findOne({"_id" : _id}))

      .then((Map oldData) {
        if (oldData == null) oldData = {};
        // check that current db state is consistent with required action
        var inferredAction;
        if (oldData.isNotEmpty && newData.isEmpty) inferredAction = 'remove';
        else if (oldData.isEmpty && newData.isNotEmpty) inferredAction = 'add';
        else if (oldData.isNotEmpty && newData.isNotEmpty) inferredAction = 'change';
        else throw new BreakException();

        _checkInferredAction(action, inferredAction, upsert);

        if (!newData.isEmpty && newData['_id'] != _id) {
          throw new MongoException(null,null,
              'New document id ${newData['_id']} should be same as old one $_id.');
        } else {
          return _maxVersion.then((version) {
            nextVersion = version + 1;
            if (inferredAction == 'remove' ){
              return collection.remove({'_id': _id});
            } else {
              newData[VERSION_FIELD_NAME] = nextVersion;
              if (inferredAction == 'add') {
                return collection.insert(newData);
              } else {
                return collection.save(newData);
              }
            }
          }).then((_) =>
            _collectionHistory.insert({
              "before" : oldData,
              "after" : stripCollectionName(newData),
              "action" : inferredAction,
              "author" : author,
              "version" : nextVersion,
              "timestamp" : new DateTime.now()
            }));
        }
      })
      .catchError((e, s) => _processError(e, s))
      )).then((_) => nextVersion);
  }

  static ensureId(Map doc, IdGenerator idgen) {
    if (!doc.containsKey('_id')) doc['_id'] = idgen.next();
  }

  static stripCollectionName(Map doc){
    //modifikuje doc
    if (doc.containsKey(COLLECTION_NAME)) doc.remove(COLLECTION_NAME);
    return doc;
  }

  Future change(String _id, Map newData, String author, {clientVersion: null, upsert: false}) {
    return writeOperation(_id, author, 'change', newData,
        clientVersion: clientVersion, upsert: upsert);      //return _maxVersion.then((version) => {'data': data, 'version': version});

  }

  Future add(Map data, String author, {clientVersion: null}) {
    ensureId(data, idgen);
    return writeOperation(data['_id'], author, 'add', data,
        clientVersion: clientVersion);
  }

  Future remove(String _id, String author, {clientVersion: null}) {
    return writeOperation(_id, author, 'remove', {}, clientVersion: null);
  }

  Future removeBySelector(Map selector, String author) {
    return find(selector).fields(["_id"]).getDataSet()
        .then((docs) => docs.map((d) => d["_id"]))
        .then((ids) => Future.forEach(ids,
            (id) => remove(id, author)
                // Documents can be removed meanwhile, do not throw in such case
                .catchError((e) => e, test: (e) => e is BreakException)));
  }

  Future changeJson(String _id, jsonData, String author, {clientVersion: null, upsert: false}) {
    cache.invalidate();

    num nextVersion;
    return mongoConn.transact((MongoDatabase mdb) =>
      mdb._operation(() =>
           new Future.sync(() => _checkClientVersion(clientVersion))
      .then((_) => collection.findOne({"_id" : _id}))
      .then((Map oldData) {
        if (oldData == null) oldData = {};
        var newData = useful.clone(oldData);

        var action;
        if(jsonData is List) {
          if(jsonData[0] == CLEAN_UNDEFINED){
            action = 'add';
            ensureId(jsonData[0], idgen);
          }
          else if(jsonData[1] == CLEAN_UNDEFINED){
            action = 'remove';
            newData = {};
          }
          else action = 'change';

          if(jsonData[1] != CLEAN_UNDEFINED) {
            newData = jsonData[1];
          }
        }
        else  {
          applyJSON(jsonData, newData);
          action = 'change';
        }

        // check that current db state is consistent with required action
        var inferredAction;
        if (oldData.isNotEmpty && newData.isEmpty) inferredAction = 'remove';
        else if (oldData.isEmpty && newData.isNotEmpty) inferredAction = 'add';
        else if (oldData.isNotEmpty && newData.isNotEmpty) inferredAction = 'change';
        else throw new BreakException();

        _checkInferredAction(action, inferredAction, upsert);

        if (!newData.isEmpty && newData['_id'] != _id) {
          throw new MongoException(null,null,
              'New document id ${newData['_id']} should be same as old one $_id.');
        } else {
          return _maxVersion.then((version) {
            nextVersion = version + 1;
            if (inferredAction == 'remove' ){
              return collection.remove({'_id': _id});
            } else {
              newData[VERSION_FIELD_NAME] = nextVersion;
              if (inferredAction == 'add') {
                return collection.insert(stripCollectionName(newData));
              } else {
                return collection.save(stripCollectionName(newData));
              }
            }
          }).then((_) =>
            _collectionHistory.insert({
              "before" : oldData,
              "after" : stripCollectionName(newData),
              "action" : inferredAction,
              "author" : author,
              "version" : nextVersion,
              "timestamp" : new DateTime.now(),
              "jsonData" : jsonData
            }));
        }
      })
      .catchError((e, s) => _processError(e, s))
      )).then((_) => nextVersion);
  }

  Future updateYielding(selector, void modifier(Map document), String author) {

    Cursor cursor = collection.find(createSelector(selector, ['_id'], []));
    Future addNext() {
      return cursor.nextObject().then((value) {
        if (value==null) {
          return new Future.value(null);
        } else {
          return update({'_id': value['_id']}, modifier, author)
               .then((_) => addNext());
        }
      });
    }
    return addNext();
  }

  Future update(selector, dynamic modifier(Map document), String author) {
    cache.invalidate();
    num nextVersion;
    List oldData;
    return mongoConn.transact((MongoDatabase mdb) =>
      mdb._operation(() => _maxVersion.then((version) {
        nextVersion = version + 1;
        num versionUpdate = nextVersion;

        Map prepare(Map document) {
          try {
            modifier(document);
          } catch(e,s) {
            throw new ModifierException(e,s);
          }
          document[VERSION_FIELD_NAME] = versionUpdate++;
          return document;
        }

        var col = collection.find(selector);
        return col.toList().then((data) {
          oldData = clone(data);
          return Future.forEach(data,
              (item) => collection.update({'_id': item['_id']},
                  prepare(item))
              );
        });
      }).then((_) {
        return Future.forEach(oldData,
          (oldItem) {
            return collection.find({'_id': oldItem['_id']}).toList().then((newItem) =>
            _collectionHistory.insert({
              "before" : oldItem,
              "after" : stripCollectionName(newItem.single),
              "action" : "change",
              "author" : author,
              "version" : nextVersion++,
              "timestamp" : new DateTime.now()
            }));
          });
        })
        .catchError( (e,s ) {
          // Errors thrown by MongoDatabase are Map objects with fields err, code,
          logger.warning('MP update error:', e, s);
          if (e is ModifierException) {
            throw e;
          } else throw new MongoException(e,s);
        })
      )).then((_) => nextVersion);
  }

  Future removeAll(query, String author) {
    cache.invalidate();
    num nextVersion;
    return mongoConn.transact((MongoDatabase mdb) =>
      mdb._operation(() => _maxVersion.then((version) {
        nextVersion = version + 1;
        return collection.find(query).toList();
      }).then((data) {
        return collection.remove(query).then((_) {
          if (data.isNotEmpty) {
            return _collectionHistory.insertAll(data.map((elem) => {
              "before" : elem,
              "after" : {},
              "action" : "remove",
              "author" : author,
              "version" : nextVersion++,
              "timestamp" : new DateTime.now()
            }).toList(growable: false));
          } else return [];
        });
      },
      onError: (e,s) {
        // Errors thrown by MongoDatabase are Map objects with fields err, code,
        logger.warning('MP removeAll error:', e, s);
        throw new MongoException(e,s);
      }
      ))).then((_) => nextVersion);
    }

  Future<Map> diffFromVersion(num version) {
    return cache.putIfAbsent('version ${collection.collectionName}', () => _maxVersion)
      .then((maxVer) {
        if (maxVer == version) {
          return {'diff': []};
        }

        addVer(Future<Map> diffRes) {
          return diffRes.then((res) {
            res['version'] = maxVer;
            return res;
          });
        }

        return cache.putIfAbsent('diff $version  $repr', () => addVer(_diffFromVersion(version)));
      });
  }

  Future<Map> _diffFromVersion(num version) {
    try{
      return __diffFromVersion(version).then((d) {
        return {'diff': d};
      });
    } on DiffNotPossibleException catch(e) {
      return data().then((d) {
        d['diff'] = null;
        return d;
      });
    }
  }

  List _prettify(List diff){
    Set seen = new Set();
    var res = [];
    for (Map change in diff.reversed) {
      if (change['_id'] is! String) {
        throw new Exception('prettify: found ID that is not String ${change}');
      }
      var id = change['_id']+change['action'];
      assert(id is String);
      if (!seen.contains(id)) {
        res.add(change);
      }
      seen.add(id);
    }
    return new List.from(res.reversed);
  }

  /// in some case not covered so far throws DiffNotPossibleException
  Future<List> __diffFromVersion(num version) {
    // selects records that fulfilled _selector before change
    Map beforeSelector = {QUERY : {}, ORDERBY : {"version" : 1}};
    // selects records that fulfill _selector after change
    Map afterSelector = {QUERY : {}, ORDERBY : {"version" : 1}};
    // selects records that fulfill _selector before or after change
    Map beforeOrAfterSelector = {QUERY : {}, ORDERBY : {"version" : 1}};

    if (_limit > NOLIMIT || _skip > NOSKIP) {
      throw new DiffNotPossibleException();
    }

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

    beforeOrAfterSelector[QUERY]['version'] = {GT: version};

    Set before, after;
    List beforeOrAfter, diff;
    // if someone wants to select field X this means, we need to select before.X
    // and after.X, also we need everythoing from the top level (version, _id,
    // author, action
    List beforeOrAfterFields = [], beforeOrAfterExcludedFields = [];
    for (String field in addFieldIfNotEmpty(this._fields, '_id')){
      beforeOrAfterFields.add('before.$field');
      beforeOrAfterFields.add('after.$field');
    }
    for (String field in this._excludeFields){
      beforeOrAfterExcludedFields.add('before.$field');
      beforeOrAfterExcludedFields.add('after.$field');
    }
    if (beforeOrAfterFields.isNotEmpty) {
      beforeOrAfterFields.addAll(['version', '_id', 'author', 'action']);
    }
        return _collectionHistory.find(createSelector(beforeOrAfterSelector,
                           beforeOrAfterFields, beforeOrAfterExcludedFields)).toList()
        .then((result) {
          beforeOrAfter = result;
          if (beforeOrAfter.isEmpty){
            throw new BreakException([]);
          } else
          return Future.wait([
            _collectionHistory.find(createSelector(beforeSelector, ['_id'], [])).toList(),
            _collectionHistory.find(createSelector(afterSelector, ['_id'], [])).toList()]);})
        .then((results) {
            before = new Set.from(results[0].map((d) => d['_id']));
            after = new Set.from(results[1].map((d) => d['_id']));
            diff = [];

            beforeOrAfter.forEach((record) {
              assert(record['version']>version);

              _stripCleanVersion(record['before']);
              _stripCleanVersion(record['after']);
              record["after"][COLLECTION_NAME] = this.collection.collectionName;
              record["before"][COLLECTION_NAME] = this.collection.collectionName;

              if(before.contains(record['_id']) && after.contains(record['_id']))
              {
                // record was changed
                diff.add({
                  "action" : "change",
                  "_id" : record["before"]["_id"],
                  "before" : record["before"],
                  "data" : record["after"],
                  "version" : record["version"],
                  "author" : record["author"],
                });
              } else if(before.contains(record['_id'])) {
                // record was removed
                diff.add({
                  "action" : "remove",
                  "_id" : record["before"]["_id"],
                  "data" : record["before"],
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
            return _prettify(diff);

    }).catchError((e, s) => _processError(e, s));
  }

  num _defaultCompare(a, b) {
    return Comparable.compare(a,b); //a.compareTo(b);
  }

  _getCompareFunction(bool reverse) {
    if (reverse) {
      return (a, b) => -1 * _defaultCompare(a, b);
    }

    return _defaultCompare;
  }

  _getComparator(Map sortParams) {
    List<Map> fields = [];

    sortParams.forEach((field, order) {
      fields.add({"name" : field, "comparator" : _getCompareFunction(order == -1)});
    });

    return (Map item1, Map item2) {
      String name;
      num result = 0;

      for (Map field in fields) {
        name = field["name"];

        result = field["comparator"](item1[name], item2[name]);

        if (result != 0) {
          break;
        }
      }

      return result;
    };
  }

  void _insertIntoSorted(List<Map> data, Map record, Map sortParams) {
    data.add(record);
    data.sort(_getComparator(sortParams));
  }

  Future<List<Map>> _limitedDiffFromVersion(List<Map> diff) {
    return collection.find(where.raw(_rawSelector).limit(_skip + _limit + diff.length)).toList().then((data) {
      return collection.find(where.raw(_rawSelector).limit(_limit).skip(_skip)).toList().then((currentData) {
        List<Map> reversedDiff = diff.reversed.toList();
        List<Map> clientData = new List.from(data);
        List<Map> clientDiff = [];
        num maxVersion = reversedDiff.isEmpty ? 0 : reversedDiff[0]["version"];
        String defaultAuthor = "_clean_";

        reversedDiff.forEach((Map change) {
          if (change["action"] == "add") {
            clientData.removeWhere((d) => d["_id"] == change["_id"]);
          }
          else if (change["action"] == "remove") {
            _insertIntoSorted(clientData, change["data"], _sortParams);
          }
          else if (change["action"] == "change") {
            Map record = clientData.firstWhere((d) => d["_id"] == change["_id"]);

            if (record == null) {
              //TODO: the record should be certainly in clientData, throw some nice exception here
            }

            record.addAll(slice(change["before"], change["data"].keys.toList()));
            clientData.sort(_getComparator(_sortParams));

            if (!record.containsKey("_metadata")) {
              record["_metadata"] = {};
            }

            change["data"].forEach((name, value) {
              if (!record["_metadata"].containsKey(name)) {
                record["_metadata"][name] = value;
              }
            });

          }
        });

        if (clientData.length > _skip) {
          clientData = clientData.getRange(_skip, [clientData.length, _skip + _limit].reduce(min)).toList();
        }
        else {
          clientData = [];
        }

        Set clientDataSet = new Set.from(clientData.map((d) => d['_id']));
        Set dataSet = new Set.from(currentData.map((d) => d['_id']));

        // as these diffs are generated from two data views (not fetched from
        // the DB), there is no way to tell the version nor author. These diffs
        // have to be applied alltogether or not at all

        clientData.forEach((Map clientRecord) {
          if (dataSet.contains(clientRecord["_id"])) {
            if (clientRecord.containsKey("_metadata")) {
              clientDiff.add({
                "action" : "change",
                "_id" : clientRecord["_id"],
                "data" : clientRecord["_metadata"],
                "version" : maxVersion,
                "author" : defaultAuthor,
              });
            }
          }
          else {
            // data does not contain the clientRecord thus it needs to be removed
            clientDiff.add({
              "action" : "remove",
              "_id" : clientRecord["_id"],
              "version" : maxVersion,
              "author" : defaultAuthor,
            });
          }
        });

        currentData.forEach((Map record) {
          if (!clientDataSet.contains(record["_id"])) {
            clientDiff.add({
              "action" : "add",
              "_id" : record["_id"],
              "data" : record,
              "version" : maxVersion,
              "author" : defaultAuthor,
            });
          }
        });

        return clientDiff;
      });
    });
  }

  void _stripCleanVersion(dynamic data) {
    if (data is Iterable) {
      data.forEach((Map item) {
        item.remove('__clean_version');
      });
    } else {
      data.remove('__clean_version');
    }
  }
}

waitingForLocks() {
  if(Zone.current[#db_lock] == null) return;
  Zone.current[#db_lock]['count']++;
  if(Zone.current[#db_lock]['count'] == 1) {
    (Zone.current[#db_lock]['stopwatch'] as Stopwatch).stop();
  }
}

gotLocks() {
  if(Zone.current[#db_lock] == null) return;
  Zone.current[#db_lock]['count']--;
  if(Zone.current[#db_lock]['count'] == 0) {
    (Zone.current[#db_lock]['stopwatch'] as Stopwatch).start();
  }
}
