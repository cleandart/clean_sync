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
  logger.fine('handleDiff: $subscription $author $diff');
  DataSet collection = subscription.collection;
  List<String> modifiedFields;
  var version = subscription._version;

  diff.forEach((Map change) {
    if(change['version'] <= version) {
      return;
    }
    change = cleanify(change);
    if (change["action"] == "add" && change["author"] != author) {
      DataMap record = collection.firstWhere((d) => d["_id"] == change["_id"], orElse : () => null);
      if (record == null) {
        collection.add(new DataMap.from(change["data"]), author: author);
      }
    }
    else if (change["action"] == "change" && change["author"] != author) {
      DataMap record = collection.firstWhere((d) => d["_id"] == change["_id"], orElse : () => null);
      // 1. the change may be for item that is currently not present in the collection;
      // 2. the field may be 'locekd', because it was changed on user's machine, and
      // this change was not yet confirmed from server
      if (record != null && subscription._modifiedItems.containsKey(record['_id'])) {
        logger.fine('discarding diff');
      }
       if (record != null && !subscription._modifiedItems.containsKey(record['_id'])) {
        applyChange(change["data"], record, author);
      }
    }
    else if (change["action"] == "remove" && change["author"] != author) {
      collection.removeWhere((d) => d["_id"] == change["_id"], author: author);
    }
  print('applying finished: $subscription ${subscription.collection} ${subscription._version}');
  });
}

class Subscription {
  // constructor arguments:
  String collectionName;
  DataSet collection;
  Connection _connection;
  String _author;
  String toString() => 'Subscription(${_author}, ver: ${_version})';
  IdGenerator _idGenerator;
  Function _handleData = handleData;
  Function _handleDiff = handleDiff;
  /// Used for testing and debugging. If true, data (instead of diff) is
  /// requested periodically.
  bool _forceDataRequesting = false;
  Map args = {};
  /// Maps _id of a document to Future, that completes when server response
  /// to document's update is completed
  Map<String, Future> _modifiedItems = {};


  num _version;
  Completer _initialSync = new Completer();
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

    markToken(id, result) {
      _modifiedItems[id] = result;
      result.then((nextVersion){
        if (_modifiedItems[id] == result) {
          _modifiedItems.remove(id);
          _version = nextVersion;
        }
      });
    }

    _subscriptions.add(collection.onChangeSync.listen((event) {
      if (event["author"] == null) {
        event["change"].addedItems.forEach((data) {
          Future result = _connection.send(() => new ClientRequest("sync", {
            "action" : "add",
            "collection" : collectionName,
            "data" : cleanify(decleanify(data)),
            "author" : _author
          }));
          markToken(data['_id'], result);
        });

        event["change"].strictlyChanged.forEach((DataMap data, ChangeSet changeSet) {
//          Map change = {};
//          changeSet.changedItems.forEach((k, Change v) => change[k] = v.newValue);
          Future result = _connection.send(() => new ClientRequest("sync", {
            "action" : "change",
            "collection" : collectionName,
            "_id": data["_id"],
            "change" : cleanify(decleanify(data)),
            "author" : _author
          }));
          // TODO: check if server really accepted the change
          markToken(data['_id'], result);
        });

        event["change"].removedItems.forEach((data) {
          Future result = _connection.send(() => new ClientRequest("sync", {
            "action" : "remove",
            "collection" : collectionName,
            "_id" : data["_id"],
            "author" : _author
          }));
          markToken(data['_id'], result);
        });
      }
    }));
  }

  _createDataRequest() => new ClientRequest("sync", {
    "action" : "get_data",
    "collection" : collectionName
  });

  _createDiffRequest() {
    return new ClientRequest("sync", {
    "action" : "get_diff",
    "collection" : collectionName,
    "version" : _version
    });
  }

  // TODO rename to something private-like
  void setupDataRequesting() {
    // request initial data
    _connection.send(_createDataRequest).then((response) {
      _version = response['version'];
      _handleData(response['data'], collection, _author);

      logger.info("Got initial data, synced to version ${_version}");

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
              _handleDiff(response['diff'], this, _author);
              _version = response['diff'].map((item) => item['version'])
                  .reduce(max);
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

  Future close() {
    dispose();
    return Future.wait(_modifiedItems.values);
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
