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
const String SET = "\$set";
const String UNSET = "\$unset";
const String PUSH = "\$push";
const num ASC = 1;
const num DESC = -1;
const num NOLIMIT = 0;
const num NOSKIP = 0;

const String VERSION_FIELD_NAME = '__clean_version';
const String LOCK_COLLECTION_NAME = '__clean_lock';
final Function historyCollectionName =
  (collectionName) => "__clean_${collectionName}_history";

/**
 * TODO: this function should be tidied up to some utilities class
 * Creates a new Map out of the given [map] preserving only keys
 * specified in [keys]
 * [map] is the Map to be sliced
 * [keys] is a list of keys to be preserved
 */
Map slice(Map map, List keys) {
  Map result = {};

  keys.forEach((key) {
    if (map.containsKey(key)) {
      result[key] = map[key];
    }
  });

  return result;
}

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
    if (keys.isNotEmpty) {
      init.add(_conn.then((_) =>
          _db.createIndex(collectionName, keys: keys, unique: unique)));
    }
  }

  MongoProvider collection(String collectionName) {
    DbCollection collection = _db.collection(collectionName);
    DbCollection collectionHistory =
        _db.collection(historyCollectionName(collectionName));
    return new MongoProvider(collection, collectionHistory, _lock);
  }

  Future dropCollection(String collectionName) =>
    _conn.then((_) => Future.wait([
      _db.collection(collectionName).drop(),
      _db.collection(historyCollectionName(collectionName)).drop()
    ]));

  Future removeLocks() => _lock.drop();
}

class MongoProvider implements DataProvider {
  final DbCollection collection, _collectionHistory, _lock;
  List<Map> _selectorList = [];
  Map _sortParams = {};
  num _limit = NOLIMIT;
  num _skip = NOSKIP;

  Future<int> get _maxVersion =>
      _collectionHistory.find(where.sortBy('version', descending : true)
          .limit(1)).toList()
      .then((data) => data.isEmpty? 0: data.first['version']);

  Map get _rawSelector => {QUERY: _selectorList.isEmpty ?
      {} : {AND: _selectorList}, ORDERBY: _sortParams};

  MongoProvider(this.collection, this._collectionHistory, this._lock);

  void _copySelection(MongoProvider mp) {
    this._sortParams = new Map.from(mp._sortParams);
    this._selectorList = new List.from(mp._selectorList);
    this._limit = mp._limit;
    this._skip = mp._skip;
  }

  Future deleteHistory(num version) {
    return _collectionHistory.remove({'version': {LT: version}});
  }

  MongoProvider find(Map params) {
    var mp = new MongoProvider(collection, _collectionHistory, _lock);
    mp._copySelection(this);
    mp._selectorList.add(params);
    return mp;
  }

  MongoProvider sort(Map params) {
    var mp = new MongoProvider(collection, _collectionHistory, _lock);
    mp._copySelection(this);
    mp._sortParams.addAll(params);
    return mp;
  }

  MongoProvider limit(num value) {
    var mp = new MongoProvider(collection, _collectionHistory, _lock);
    mp._copySelection(this);
    mp._limit = value;
    return mp;
  }

  MongoProvider skip(num value) {
    var mp = new MongoProvider(collection, _collectionHistory, _lock);
    mp._copySelection(this);
    mp._skip = value;
    return mp;
  }

  /**
   * Returns data and version of this data 7.
   */
  Future<Map> data({projection: null, stripVersion: true}) {
    return collection.find(where.raw(_rawSelector).limit(_limit).skip(_skip)).toList().then((data) {
      //return _maxVersion.then((version) => {'data': data, 'version': version});
      var version = data.length == 0 ? 0 : data.map((item) => item['__clean_version']).reduce(max);
      if(stripVersion) _stripCleanVersion(data);
      if (projection != null){
        data.forEach((e) => projection(e));
      }
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
      ).then((_) => _release_locks()).then((_) => nextVersion);
  }

  Future addAll(List<Map> data, String author) {
    num nextVersion;
    return _get_locks().then((_) => _maxVersion).then((version) {
        nextVersion = version + 1;
        data.forEach((elem) => elem[VERSION_FIELD_NAME] = nextVersion++);
        return collection.insertAll(data);
      }).then((_) =>
        _collectionHistory.insertAll(data.map((elem) =>
            {
              "before" : {},
              "after" : elem,
              "action" : "add",
              "author" : author,
              "version" : elem[VERSION_FIELD_NAME]
            }).toList(growable: false)),
      onError: (e) {
        // Errors thrown by MongoDatabase are Map objects with fields err, code,
        // ...
        return _release_locks().then((_) {
          throw new MongoException(e);
        });
      }
      ).then((_) => _release_locks()).then((_) => nextVersion);
  }

  Future deprecatedChange(String _id, Map change, String author) {
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
      ).then((_) => _release_locks()).then((_) => nextVersion);
  }

  //TODO: change means new data, rename it
  Future change(String _id, Map change, String author) {
    num nextVersion;
    Map newRecord;
    return _get_locks().then((_) => collection.findOne({"_id" : _id}))
      .then((Map record) {
        if(record == null) {
          return true;
//          throw new MongoException(null,
//              'Change was not applied, document with id $_id does not exist.');
        } else if (change.containsKey('_id') && change['_id'] != _id) {
          throw new MongoException(null,
              'New document id ${change['_id']} should be same as old one $_id.');
        } else {
          return _maxVersion.then((version) {
            nextVersion = version + 1;
            newRecord = change;
            newRecord[VERSION_FIELD_NAME] = nextVersion;
            return collection.save(newRecord);
          }).then((_) =>
            _collectionHistory.insert({
              "before" : record,
              "after" : newRecord,
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
      ).then((_) => _release_locks()).then((_) => nextVersion);
  }

  Future update(selector,Map document, String author, {bool upsert: false, bool multiUpdate: false, WriteConcern writeConcern}) {
    num nextVersion;
    List oldData;
    return _get_locks().then((_) => _maxVersion).then((version) {
        nextVersion = version + 1;
        num versionUpdate = nextVersion;
        var prepare;
        if(document.keys.any((K) => K.startsWith('\$'))) {
          prepare = (doc) {
            doc[SET][VERSION_FIELD_NAME] =  versionUpdate++;
            return doc;
          };
          if(!document.containsKey(SET))
            document[SET] = {};
        }
        else {
          prepare = (doc) {
            doc[VERSION_FIELD_NAME] =  versionUpdate++;
            return doc;
          };
        }

        var col = collection.find(selector);
        return col.toList().then((data) {
          oldData = data;
          return Future.forEach(data,
              (item) => collection.update({'_id': item['_id']},
                  prepare(document), upsert: upsert, multiUpdate: multiUpdate,
                  writeConcern: writeConcern));
        });
      }).then((_) {
        return Future.forEach(oldData,
          (oldItem) {
            return collection.find({'_id': oldItem['_id']}).toList().then((newItem) =>
            _collectionHistory.insert({
              "before" : oldItem,
              "after" : newItem.single,
              "action" : "change",
              "author" : author,
              "version" : nextVersion++
            }));
          });
        }).then((_) => _release_locks()).then((_) => nextVersion)
        .catchError( (e) {
          // Errors thrown by MongoDatabase are Map objects with fields err, code,
          // ...
          return _release_locks().then((_) {
            throw new MongoException(e);
          });
        });
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
      ).then((_) => _release_locks()).then((_) => nextVersion);
  }

  Future removeAll(query, String author) {
    num nextVersion;
    return _get_locks().then((_) => _maxVersion).then((version) {
        nextVersion = version + 1;
        return collection.find(query).toList();
      }).then((data) {
        return collection.remove(query).then((_) =>
          _collectionHistory.insertAll(data.map((elem) => {
            "before" : elem,
            "after" : {},
            "action" : "remove",
            "author" : author,
            "version" : nextVersion++
        }).toList(growable: false)));
      },
      onError: (e) {
        // Errors thrown by MongoDatabase are Map objects with fields err, code,
        // ...
        return _release_locks().then((_) {
          throw new MongoException(e);
        });
      }
      ).then((_) => _release_locks()).then((_) => nextVersion);
  }

  Future<Map> diffFromVersion(num version, {projection: null}) {
    try{
      return _diffFromVersion(version, projection: projection).then((d) {
        return d;
      });
    } on DiffNotPossibleException catch(e) {
      return data(projection: projection).then((d) {
        d['diff'] = null;
        return d;
      });
    }
  }

  List pretify(List diff){
    Set seen = new Set();
    var res = [];
    for (Map change in diff.reversed) {
      var id = change['_id']+change['action'];
      assert(id is String);
      if (!seen.contains(id)) {
        res.add(change);
      }
      seen.add(id);
    }
    return new List.from(res.reversed);
  }

  Future<Map> _diffFromVersion(num version, {projection:null}) {
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

     return _maxVersion.then((maxVersion) {
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
              assert(record['version']>version);

              _stripCleanVersion(record['before']);
              _stripCleanVersion(record['after']);

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

            if (_limit > NOLIMIT || _skip > NOSKIP) {
              throw new Exception('not correctly implemented');
              return _limitedDiffFromVersion(diff);
            }

            if (projection!=null) {
              for (Map elem in diff) {
                if(elem.containsKey('data')){
                  projection(elem['data']);
                }
              }
            }

            if (diff.isEmpty) {
              return {'diff' : [], 'version' : maxVersion};
            } else {
              return {'diff' : pretify(diff)};
            }
        });
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


