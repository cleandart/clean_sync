// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

void handleData(List<Map> data, DataSet collection, String author) {
  collection.clear(author: author);
  List<DataMap> toAdd = [];
  for (Map record in data) {
    toAdd.add(new DataMap.from(record));
  }
  collection.addAll(toAdd, author: author);
}

void _applyChangeList (List source, DataList target, author) {
  target.length = source.length;
  for (num i=0; i<target.length; i++) {
    if (!applyChange(source[i], target[i], author)) {
      // TODO add set method to DataList and use it here
      target.setAll(i, [source[i]], author: author);
    }
  }
}

void _applyChangeMap (Map source, DataMap target, author) {
  for (var key in new List.from(source.keys)) {
    if (target.containsKey(key)) {
      if(!applyChange(source[key], target[key], author)){
        target.add(key, source[key], author: author);
      }
    } else {
      target.add(key, source[key], author: author);
    }
  }
  for (var key in new List.from(target.keys)) {
    if (!source.containsKey(key)) {
      target.remove(key, author: author);
    }
  }
}

bool applyChange (source, target, author) {
  if (source is Map && target is Map) {
    _applyChangeMap(source, target, author);
    return true;
  }
  if (source is List && target is List) {
    _applyChangeList(source, target, author);
    return true;
  }
  if(source == target) {
    return true;
  }
  return false;
}



void handleDiff(List<Map> diff, Subscription subscription, String author) {
  DataSet collection = subscription.collection;
  List<String> modifiedFields;
  var profiling = new Stopwatch()..start();

  diff.forEach((Map change) {
    if (change["action"] == "add") {
      DataMap record = collection.firstWhere((d) => d["_id"] == change["_id"], orElse : () => null);
      if (record == null) {
        collection.add(new DataMap.from(cleanify(change["data"])), author: author);
      }
    }
    else if (change["action"] == "change" && change["author"] != author) {
      DataMap record = collection.firstWhere((d) => d["_id"] == change["_id"], orElse : () => null);
      if (record != null) {
        modifiedFields = subscription.modifiedDataFields(record);
        applyChange(cleanify(change["data"]), record, author);

//        change["data"].forEach((String key, dynamic value) {
//          if (!modifiedFields.contains(key)) {
//            record.add(key, value, author: author);
//          }
//        });
      }
    }
    else if (change["action"] == "remove") {
      collection.removeWhere((d) => d["_id"] == change["_id"], author: author);
    }
//    print("handleDiff:${profiling.elapsed}");
    profiling.stop();
  });
}

class Subscription {
  // constructor arguments:
  String collectionName;
  DataSet collection;
  Connection _connection;
  String _author;
  IdGenerator _idGenerator;
  Function _handleData = handleData;
  Function _handleDiff = handleDiff;
  /// Used for testing and debugging. If true, data (instead of diff) is
  /// requested periodically.
  bool _forceDataRequesting = false;
  Map args = {};

  num _version;
  Completer _initialSync = new Completer();
  Map<String, Map<String, num>> _modifiedFields = {};
  List<StreamSubscription> _subscriptions = [];

  /// Completes after first request to get data is answered and handled.
  Future get initialSync => _initialSync.future;

  Subscription.config(this.collectionName, this.collection, this._connection,
      this._author, this._idGenerator, this._handleData, this._handleDiff,
      this._forceDataRequesting, [this.args]);

  Subscription(this.collectionName, this._connection, this._author,
      this._idGenerator, [this.args]) {
    collection = new DataSet();
    start();
  }

  void _initDataField(DataMap data, String field) {
    if (!_modifiedFields.containsKey(data["_id"])) {
      _modifiedFields[data["_id"]] = {};
    }

    if (!_modifiedFields[data["_id"]].containsKey(field)) {
      _modifiedFields[data["_id"]][field] = 0;
    }
  }

  num tokenForDataField(DataMap data, String field) {
    _initDataField(data, field);
    return _modifiedFields[data["_id"]][field];
  }

  num nextTokenForDataField(DataMap data, String field) {
    _initDataField(data, field);
    _modifiedFields[data["_id"]][field] += 1;
    return _modifiedFields[data["_id"]][field];
  }

  List<String> modifiedDataFields(DataMap data) {
    if (_modifiedFields.containsKey(data["_id"])) {
      return _modifiedFields[data["_id"]].keys.toList();
    }
    else {
      return [];
    }
  }

  void _clearTokenForDataField(DataMap data, String field) {
    if (_modifiedFields.containsKey(data["_id"])) {
      _modifiedFields[data["_id"]].remove(field);

      if (_modifiedFields[data["_id"]].isEmpty) {
        _modifiedFields.remove(data["_id"]);
      }
    }
  }

  /**
   * Waits for initialSync of all provided subscriptions.
   */
  static Future wait(List<Subscription> subscriptions) {
    return Future.wait(
        subscriptions.map((subscription) => subscription.initialSync));
  }

  // TODO rename to something private-like
  void setupListeners() {
    _subscriptions.add(collection.onBeforeAdd.listen((data) {
      // if data["_id"] is null, it was added by this client and _id should be
      // assigned
      if(data["_id"] == null) {
        data["_id"] = _idGenerator.next();
      }
    }));

    _subscriptions.add(collection.onChangeSync.listen((event) {
      if (event["author"] == null) {
        event["change"].addedItems.forEach((data) {
          _connection.send(() => new ClientRequest("sync", {
            "action" : "add",
            "collection" : collectionName,
            "data" : data,
            "author" : _author
          }));
        });

        event["change"].strictlyChanged.forEach((DataMap data, ChangeSet changeSet) {
          Map change = {};
          changeSet.changedItems.
            forEach((k, Change v) => change[k] = v.newValue);

          Map<String, num> tokens = {};

          change.keys.forEach((field) {
            tokens[field] = nextTokenForDataField(data, field);
          });

          _connection.send(() => new ClientRequest("sync", {
            "action" : "change",
            "collection" : collectionName,
            "_id": data["_id"],
            "change" : change,
            "author" : _author
          })).then((_) {
            // TODO: check if server really accepted the change
            tokens.forEach((field, token) {
              if (tokenForDataField(data, field) == token) {
                _clearTokenForDataField(data, field);
              }
            });
          });
        });

        event["change"].removedItems.forEach((data) {
          _connection.send(() => new ClientRequest("sync", {
            "action" : "remove",
            "collection" : collectionName,
            "_id" : data["_id"],
            "author" : _author
          }));
        });
      }
    }));
  }

  _createDataRequest() => new ClientRequest("sync", {
    "action" : "get_data",
    "collection" : collectionName
  });

  _createDiffRequest() => new ClientRequest("sync", {
    "action" : "get_diff",
    "collection" : collectionName,
    "version" : _version
  });

  // TODO rename to something private-like
  void setupDataRequesting() {
    // request initial data
    _connection.send(_createDataRequest).then((response) {
      _version = response['version'];
      _handleData(response['data'], collection, _author);

      print("Got initial data, synced to version ${_version}");

      // TODO remove the check? (restart/dispose should to sth about initialSynd)
      if (!_initialSync.isCompleted) _initialSync.complete();

      var subscription = _connection
        .sendPeriodically(_forceDataRequesting ?
            _createDataRequest : _createDiffRequest)
        .listen((response) {
          // id data and version was sent, diff is set to null
          if(response['diff'] == null) {
            _version = response['version'];
            _handleData(response['data'], collection, _author);
          } else {
            if(!response['diff'].isEmpty) {
              _version = response['diff'].map((item) => item['version'])
                  .reduce(max);
              _handleDiff(response['diff'], this, _author);
            }
          }
        });

      _subscriptions.add(subscription);
    });
  }

  void start() {
    setupListeners();
    setupDataRequesting();
  }

  void dispose() {
    _subscriptions.forEach((s) => s.cancel());
  }

  void restart() {
    dispose();
    start();
  }

  Stream onClose() {

  }
}
