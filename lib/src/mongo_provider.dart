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

/// Canonical way of creating name of the history collection from the name of
/// the collection.
String historyCollectionName(collectionName) =>
    "__clean_${collectionName}_history";


class MongoConnection {
  Db _db;
  LockRequestor _lockRequestor;
  Cache cache;
  bool _initialized = false;
  final Duration timeout;

  static const _dbLock = "dblock";

  Db get rawDb => _db;

  /// Creates a connection to the database specified by [url] (for example
  /// 'mongodb://0.0.0.0/test:27017'). [LockRequestor] has to be provided to
  /// ensure transactions in db operations (other than per-document ones that
  /// are provided by default in mongo). All [MongoConnection] instances that
  /// work with the same database at the same time should have a [LockRequestor]
  /// that connects to the same host and port.
  /// The argument [timeout] specifies how long should one wait for the
  /// db lock when performing a transaction. (It is passed to
  /// [LockRequestor.withLock] function as [timeout].)
  MongoConnection(String url, LockRequestor this._lockRequestor,
      {Cache this.cache: dummyCache, Duration this.timeout: null}) {
    _db = new Db(url);
  }

  /// Returns a [Future] of a connection to mongodb that does not lock the
  /// database (and thus does not guarantee transactions).
  static Future<MongoConnection> noLocking(String url,
      {Cache cache: dummyCache, int port: 27005}) {
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

  /// Opens the connection to the database. Throws if the connection is open
  /// already. (Otherwise, several connections could be open and
  /// [_db.close] would have to be called several times to close all of them).
  Future init() {
    if (_initialized) {
      throw new Exception('MongoConnection.init was already called.');
    }
    _initialized = true;
    return _db.open();
  }

  /// Performs a database operation specified in [callback] using method
  /// [LockRequestor.withLock], which ensures that all db operations specified
  /// in [callback] are executed in one transaction.
  /// The database lock is specified by the constant string [_dbLock]
  /// and thus it ensures transactions among all users of lock requestors that
  /// connect to the same url and port (i.e. to the same [Locker]).
  Future transact(callback(MongoDatabase _), {String author: null}) {
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
      , timeout: timeout, metaData: {'db' : mdb}, safe: false, author: author);
  }

  /// Returns a [MongoProvider] that provides access to collection with name
  /// [collectionName].
  MongoProvider collection(String collectionName) {
    DbCollection collection = _db.collection(collectionName);
    DbCollection collectionHistory =
        _db.collection(historyCollectionName(collectionName));
    return new MongoProvider(this, collection, collectionHistory, cache);
  }

  /// Close connection to the database.
  Future close() => _db.close();

}

class MongoDatabase {
  Db _db;
  Cache cache;
  MongoConnection connection;
  Db get rawDb => _db;
  List<Future> operations = [];
  bool _disposed = false;

  /// Wraps some of mongo_dart functions for managing mongo collections for
  /// clean_sync purposes. That is, each operation on a mongo collection via
  /// MongoDatabase results in necessary operations also on the corresponding
  /// operations on the history collection.
  ///
  /// For example, each collection in mongo has automatically index on the _id
  /// field. When creating the collection via [MongoDatabase.createCollection],
  /// the history collection is also created and indexes on before._id and
  /// after._id are created automatically.
  MongoDatabase(Db this._db, MongoConnection this.connection, {Cache this.cache: dummyCache} );

  /// Waits for all operations in progress and then enters a disposed state.
  Future dispose() => Future.wait(operations).then((_) => _disposed = true);

  Future _logOperation(Future op()) {
    if (_disposed) throw new Exception("MongoDatabase is already disposed, no operations should be executed");
    Completer completer = new Completer();
    operations.add(completer.future);
    return op().whenComplete(() => completer.complete());
  }

  /**
   * Creates a collection [collectionName] along with its history collection.
   * Creates indexes on prefered fields on history automatically.
   * If [expireHistoryAfter] is set, documents in history are only retained for the set duration.
   */
  Future createCollection(String collectionName, {Duration expireHistoryAfter}) {
    var histColName = historyCollectionName(collectionName);
    return _logOperation(
        () => _db.createIndex(histColName, key: 'version', unique: true)
        .then((_) => _db.createIndex(histColName, key: 'clientVersion', unique: true, sparse: true))
        .then((_) => _db.createIndex(histColName, keys: {'before._id': 1, 'version': 1}, unique: true))
        .then((_) => _db.createIndex(histColName, keys: {'after._id': 1, 'version': 1}, unique: true))
        .then((_) => expireHistoryAfter != null ? _createTtlIndex(histColName, "timestamp", expireHistoryAfter) : null)
    );
  }

  ///Creates a TTL type of asc index on a key (doesn't work for multiple keys)
  Future _createTtlIndex(String collectionName, String key, Duration expireAfterSeconds) {
    return _logOperation(() {

          Map selector = {"name": "_${key}_1",
                          "key": { key: 1},
                          "expireAfterSeconds": expireAfterSeconds.inSeconds,
                          'ns' : '${rawDb.databaseName}.$collectionName'};

          // Behaviour copied from Db.createIndex
          MongoInsertMessage insertMessage = new MongoInsertMessage(
              '${rawDb.databaseName}.${DbCommand.SYSTEM_INDEX_COLLECTION}',[selector]);

          rawDb.executeDbCommand(insertMessage);
          return rawDb.getLastError();
    });
  }

  /**
   * Creates index on chosen collection and (if argument [history] is set to
   * [true]) corresponding indexes on collection history.
   * [keys] is a [Map] in form {field_name: 1 or -1} with 1/-1 specifying
   * ascending/descending order (same as the map passed to mongo function
   * ensureIndex).
   */
  Future createIndex(String collectionName, Map keys, {unique: false,
    sparse: false, history: true}) {
    if (keys.isEmpty) return _logOperation(() => new Future.value(null));
    var todo = [() => _db.createIndex(collectionName, keys: keys, unique: unique, sparse: sparse)];

    if (history) {
      Map beforeKeys = {};
      Map afterKeys = {};
      keys.forEach((key, val) {
        beforeKeys['before.$key'] = val;
        afterKeys['after.$key'] = val;
      });
      beforeKeys['version'] = 1;
      afterKeys['version'] = 1;

      todo.add(() => _db.createIndex(historyCollectionName(collectionName), keys: beforeKeys));
      todo.add(() => _db.createIndex(historyCollectionName(collectionName), keys: afterKeys));
    }
    return _logOperation(() => Future.forEach(todo, (f) => f()));
  }

  /// Returns a [MongoProvider] that provides data and diffs that are in
  /// mongodb collection (and corresponding history collection).
  MongoProvider collection(String collectionName) =>
      connection.collection(collectionName);

  /// Drops the collection and the corresponding histor collection in mongodb.
  Future dropCollection(String collectionName) =>
      _logOperation(() => Future.wait([
      _db.collection(collectionName).drop(),
      _db.collection(historyCollectionName(collectionName)).drop()
    ]));

   Future _operation(callback()) =>
       _logOperation(() => new Future.sync(callback));

}

List _addFieldIfNotEmpty(List fields, String field){
  if (fields.isNotEmpty) {
    var res = new List.from(fields)..add(field);
    return res;
  } else {
    return fields;
  }
}

MongoProvider _mpClone(MongoProvider source){

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

  /// This getter is public only for testing purposes, do not use it.
  Future<int> get maxVersion => _maxVersion;

  Future<int> get _maxVersion =>
      _collectionHistory.find(where.sortBy('version', descending : true)
          .limit(1)).toList()
      .then((data) => data.isEmpty? 0: data.first['version']);

  Map get _rawSelector => {QUERY: _selectorList.isEmpty ?
      {} : {AND: _selectorList}, ORDERBY: _sortParams};

  /// Wraps some of mongo_dart functions for operations on mongo collections,
  /// ensuring that the history of the collection modifications is recorded.
  ///
  /// For example, when a document is added to a collection via
  /// [MongoProvider.add], the corresponding document is added to the history
  /// collection.
  ///
  /// Provided [collection] and [collectionHistory] are the underlying
  /// mongodb collections used to handle the documents.
  MongoProvider(MongoConnection mongoConn, DbCollection collection,
      DbCollection collectionHistory, Cache cache) :
    this.config(mongoConn, collection, collectionHistory, cache,
        new IdGenerator(getIdPrefix()));

  /// Same as [MongoProvider] with an option to specify custom [IdGenerator].
  MongoProvider.config(MongoConnection this.mongoConn,
      DbCollection this.collection, DbCollection this._collectionHistory,
      Cache this.cache, IdGenerator this.idgen);

  /// Delete all history documents older than version.
  Future deleteHistory(num version) {
    return _collectionHistory.remove({'version': {LT: version}});
  }

  /// Returns a MongoProvider whose data-getting methods (for example
  /// [MongoProvider.data]) return documents that contain only the specified
  /// [fields].
  ///
  /// Can be combined with [excludeFields], [find], [sort]...
  ///
  /// Example:
  /// Let [mp] be a [MongoProvider] whose collection contains one document
  /// {"_id":0, "a":1, "b":5}. Then [mp.fields(["a"]).data()] would return
  /// a Future of {"_id":0, "a":1}.
  MongoProvider fields(List<String> fields) {
    var res = _mpClone(this);
    res._fields.addAll(fields);
    return res;
  }

  /// Returns a MongoProvider whose data-getting methods (for example
  /// [MongoProvider.data]) return documents that exclude the specified
  /// [excludeFields].
  ///
  /// Can be combined with [fields], [find], [sort]...
  ///
  /// Example:
  /// Let [mp] be a [MongoProvider] whose collection contains one document
  /// {"_id":0, "a":1, "b":5}. Then [mp.excludeFields(["a"]).data()] would
  /// return a Future of {"_id":0, "b":5}.
  MongoProvider excludeFields(List<String> excludeFields) {
    var res = _mpClone(this);
    res._excludeFields.addAll(excludeFields);
    return res;
  }

  /// Returns a MongoProvider whose data-getting methods (for example
  /// [MongoProvider.data]) return documents that exclude the specified
  /// [excludeFields].
  ///
  /// Can be combined with [excludeFields], [fields], [sort]...
  ///
  /// Example:
  /// Let [mp] be a [MongoProvider] whose collection contains two documents
  /// {"_id":0, "a":1, "b":5} and {"_id":0, "a":3, "b":1}. Then
  /// [mp.find({"a": 1}).data()] would return a Future of
  /// {"_id":0, "a":1, "b":5}.
  MongoProvider find([Map params = const {}]) {
    var res = _mpClone(this);
    res._selectorList.add(params);
    return res;
  }

  /// Returns a MongoProvider whose data-getting methods (for example
  /// [MongoProvider.data]) return document sorted according to [params].
  /// The form of [params] is analogical to the arguments used for mongodb's
  /// sort method.
  MongoProvider sort(Map params) {
    var res = _mpClone(this);
    res._sortParams.addAll(params);
    return res;
  }

  /// Returns a MongoProvider whose data-getting methods (for example
  /// [MongoProvider.data]) return only first [value] documents. Can be used
  /// in combination with [sort], [skip], [fields], ...
  MongoProvider limit(num value) {
    var res = _mpClone(this);
    res._limit = value;
    return res;
  }

  /// Returns a MongoProvider whose data-getting methods (for example
  /// [MongoProvider.data]) return document without the first [value] documents.
  /// Can be used in combination with [sort], [limit], [fields], ...
  MongoProvider skip(num value) {
    var res = _mpClone(this);
    res._skip = value;
    return res;
  }

//  dynamic repr(String operation) {
//    return Tpl();
//  }

  /// Returns string representation of this [MongoProvider] for debug/logging
  /// purposes.
  String get repr {
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

  /// See [DataProvider.data] specification.
  Future<Map> data({stripVersion: true}) {
    return cache.putIfAbsent('data $repr', () => _data(stripVersion: stripVersion));
  }

  /// Helper function that fetches data from the collection and creates
  /// clean_data [DataSet] from it.
  Future<DataSet> getDataSet() {
    return data().then((data) => new DataSet.from(data['data'])..addIndex(['_id']));
  }


  _createSelector(Map selector, List fields, List excludeFields) {
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
    var __fields = _addFieldIfNotEmpty(_fields, VERSION_FIELD_NAME);
    return _createSelector(_rawSelector, __fields, _excludeFields)
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


  /// Adds several documents, specified in [data], to the collection and adds
  /// the corresponding records to its history, specifying [author] as the
  /// author of the change.
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
      )), author: author).then((_) => nextVersion);
  }


  Future _writeOperation(String _id, String author, String action, Map newData,
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
      ), author: author).then((_) => nextVersion);
  }

  /// If the provided document [doc] does not contain field "_id", modifies
  /// the document by adding an "_id" field, using [idgen] to generate its
  /// value.
  static ensureId(Map doc, IdGenerator idgen) {
    if (!doc.containsKey('_id')) doc['_id'] = idgen.next();
  }

  /// If the provided document [doc] contains internal clean_sync meta-field
  /// [COLLECTION_NAME], removes it from the document.
  static stripCollectionName(Map doc){
    //modifikuje doc
    if (doc.containsKey(COLLECTION_NAME)) doc.remove(COLLECTION_NAME);
    return doc;
  }

  /// Changes the document with id [_id] to [newData] in the collection and adds
  /// a new record to the collection history (specifying [author] as the author
  /// of the change).
  ///
  /// Throws if the document with id [_id] is not present in the database and
  /// [upsert] is [false]. If [upsert] is set to [true], the document [newData]
  /// with id [_id] is added to the database (and history).
  ///
  /// [clientVersion] is used by the client-side clean_sync library and does not
  /// have to be used when calling this method on server side.
  Future change(String _id, Map newData, String author, {clientVersion: null,
    upsert: false}) {
    return _writeOperation(_id, author, 'change', newData,
        clientVersion: clientVersion, upsert: upsert);      //return _maxVersion.then((version) => {'data': data, 'version': version});

  }

  /// Adds document [data] to the collection and the corresponding record to the
  /// collection history (specifying [author] of the addition).
  ///
  /// [clientVersion] is used by the client-side clean_sync library and does not
  /// have to be used when calling this method on server side.
  Future add(Map data, String author, {clientVersion: null}) {
    ensureId(data, idgen);
    return _writeOperation(data['_id'], author, 'add', data,
        clientVersion: clientVersion);
  }

  /// Removes the document with id [_id] from the collection and adds the
  /// corresponding record to the collection history (specifying [author] of the
  /// removal).
  ///
  /// [clientVersion] is used by the client-side clean_sync library and does not
  /// have to be used when calling this method on server side.
  Future remove(String _id, String author, {clientVersion: null}) {
    return _writeOperation(_id, author, 'remove', {}, clientVersion: null);
  }

  /// Removes all documents from collection that fulfill the [selector] using
  /// [MongoProvider.remove]. This is a helper function.
  ///
  /// [selector] is used as an argument to [MongoProvider.find] to find the
  /// documents to be removed.
  Future removeBySelector(Map selector, String author) {
    return find(selector).fields(["_id"]).getDataSet()
        .then((docs) => docs.map((d) => d["_id"]))
        .then((ids) => Future.forEach(ids,
            (id) => remove(id, author)
                // Documents can be removed meanwhile, do not throw in such case
                .catchError((e) => e, test: (e) => e is BreakException)));
  }

  /// Applies an operation encoded in [jsonData] to the mongodb collection
  /// (and adds the corresponding record to the collection history).
  ///
  /// [_id] is the id of the modified document, [author] is author of the
  /// change. [clientVersion] is used by the client-side clean_sync library and
  /// does not have to be used when calling this method on server side.
  /// [jsonData] can be a 2-element [List] or [Map]. In the first case the
  /// elements are "from" and "to" versions of the data; using
  /// [CLEAN_UNDEFINED] in the list expresses adds or removes. In the latter
  /// case, the [Map] is just the "to" version of the data (and the action is
  /// "change" by convention).
  Future changeJson(String _id, jsonData, String author, {clientVersion: null,
    upsert: false}) {
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
      ), author: author).then((_) => nextVersion);
  }

  /// Calls [MongoProvider.update] on all documents that match the [selector].
  /// This is not performed in one transaction; the function "yields" in
  /// between each two updates. This is suitable for updates that might take
  /// a long time; one ensures that the database is not unresponsive for too
  /// long. [modifier] and [author] are just passed to [MongoProvider.update].
  Future updateYielding(selector, void modifier(Map document), String author) {

    Cursor cursor = collection.find(_createSelector(selector, ['_id'], []));
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

  /// For all documents in the collection that match [selector], change the
  /// document by [modifier] and save the new version of the document instead.
  /// The corresponding "change" record is added to the collection history,
  /// [author] is the author of the change.
  ///
  /// Caution: [modifier] is used to modify the document (its return value is
  /// irrelevant, the state of the document after calling [modifier(document)]
  /// is saved to the database).
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
      ), author: author).then((_) => nextVersion);
  }

  /// Remove all documents that match [query] from the database. Add the
  /// corresponding record to the history collection, specifying [author] as the
  /// author of the change.
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
      )), author: author).then((_) => nextVersion);
    }

  /// See [DataProvider.diffFromVersion] specification.
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

  /// in some case not covered so far throws DiffNotPossibleException
  Future<List> __diffFromVersion(num version) {
    if (_limit > NOLIMIT || _skip > NOSKIP) {
      throw new DiffNotPossibleException();
    }

    return maxVersion.then((maxV) {
      // {before: {GT: {}}} to handle selectors like {before.age: null}
      List<Map> _beforeSelector = [{"version" : {GT : version, LTE: maxV}}, {"before" : {GT: {}}}];
      List<Map> _afterSelector = [{"version" : {GT : version, LTE: maxV}}, {"after" : {GT: {}}}];

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

      // selects records that fulfilled _selector before change
      Map beforeSelector = {QUERY : {AND: _beforeSelector}, ORDERBY : {"version" : 1}};;
      // selects records that fulfill _selector after change
      Map afterSelector = {QUERY : {AND: _afterSelector}, ORDERBY : {"version" : 1}};

      List before, after;

      _s(selector, fields, excludeFields, prefix) {
        // if someone wants to select field X this means, we need to select before.X
        // and after.X, also we need everything from the top level (version, _id,
        // author, action
        List _fields = [], _excludeFields = [];
        for (String field in fields) {
          _fields.add('$prefix.$field');
        }
        for (String field in excludeFields){
          _excludeFields.add('$prefix.$field');
        }
        if (_fields.isNotEmpty) {
          _fields.addAll(['version', '_id', 'author', 'action', '$prefix._id']);
        }
        return _createSelector(selector, _fields, _excludeFields);
      }

      _prepare(d) {
        if (d == null) return null;
        _stripCleanVersion(d);
        d[COLLECTION_NAME] = this.collection.collectionName;
        return d;
      }

      return Future.wait([
        _collectionHistory.find(_s(beforeSelector, this._fields, this._excludeFields, 'before')).toList(),
        _collectionHistory.find(_s(afterSelector, this._fields, this._excludeFields, 'after')).toList()])
        .then((results) {
          before = results[0];
          after = results[1];

          // add dummy stop-records to the end (used to signal when the
          // iteration should be stopped; not used to produce the diff)
          before.add({"version": maxV + 1, "stop": true});
          after.add({"version": maxV + 1, "stop": true});

          var a = 0, b = 0;
          Map docsBefore = {}, docsAfter = {}, meta = {};

          while (true) {
            var da = after[a];
            var db = before[b];

            // when both indexes come to the stop document, break
            if (da["stop"] == true && db["stop"] == true) break;

            if (da["version"] == db["version"]) {
              // record was changed
              var id = da["after"]["_id"];
              if (!docsBefore.keys.contains(id)) {
                docsBefore[id] = db["before"];
              }
              docsAfter[id] = da["after"];
              meta[id] = {"version": da["version"], "author": da["author"]};
              a++; b++;
            } else if (da["version"] > db["version"]) {
              // record was removed
              var id = db["before"]["_id"];
              if (!docsBefore.keys.contains(id)) {
                docsBefore[id] = db["before"];
              }
              docsAfter[id] = null;
              meta[id] = {"version": db["version"], "author": db["author"]};
              b++;
            } else if (da["version"] < db["version"]) {
              // record was added
              var id = da["after"]["_id"];
              if (!docsBefore.keys.contains(id)) {
                docsBefore[id] = null;
              }
              docsAfter[id] = da["after"];
              meta[id] = {"version": da["version"], "author": da["author"]};
              a++;
            } else {
              throw new Exception("Should not get here; bug in clean_sync?");
            }
          }

          return docsBefore.keys.map((id) {
            var a = _prepare(docsAfter[id]);
            var b = _prepare(docsBefore[id]);
            var action, data, before = null;

            if (a != null && b != null) {
              action = "change";
              data = a;
              before = b;
            } else if (a != null) {
                action = "add";
                data = a;
            } else {
              action = "remove";
              data = b;
            }

          var d = {"_id": id,
                   "action": action,
                   "data": data,
                   "version": meta[id]["version"],
                   "author": meta[id]["author"],
                  };

          return d;

          }).toList();

      }).catchError((e, s) => _processError(e, s));
    });
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
