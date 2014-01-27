// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

final Logger logger = new Logger('clean_sync');

void handleData(List<Map> data, DataSet collection, String author) {
  collection.clear(author: 'clean_sync');
  List<DataMap> toAdd = [];
  for (Map record in data) {
    toAdd.add(new DataMap.from(record));
  }
  collection.addAll(toAdd, author: 'clean_sync');
}

void _applyChangeList (List source, DataList target, author) {
  target.length = source.length;
  for (num i=0; i<target.length; i++) {
    if (!applyChange(source[i], target[i], author)) {
      target.set(i, source[i], author: author);
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



num handleDiff(List<Map> diff, Subscription subscription, String author) {
  logger.fine('handleDiff: $subscription $author $diff');
  DataSet collection = subscription.collection;
  List<String> modifiedFields;
  var version = subscription._version;
  num res = -1;
  bool collectRes = true;

  diff.forEach((Map change) {
//     it can happen, that we get too old changes
    if (!change.containsKey('version')){
      logger.warning('change does not contain "version" field. If not testing, '
                     'this is probably bug. (change: $change)');
      change['version'] = 0;
    } else if (version == null) {
      logger.warning('Subscription $subscription version is null. If not testing, '
                     'this is probably bug.');
    } else if(change['version'] <= version) {
      return;
    }
    change = cleanify(change);
      if (change["action"] == "add") {
      DataMap record = collection.firstWhere((d) => d["_id"] == change["_id"], orElse : () => null);
      if (collectRes) res = max(res, change['version']);
      if (record == null) {
        logger.fine('aplying changes!');
        collection.add(new DataMap.from(change["data"]), author: 'clean_sync');
      } else {
      }
    }
      else if (change["action"] == "change" ) {
      DataMap record = collection.firstWhere((d) => d["_id"] == change["_id"], orElse : () => null);
      // 1. the change may be for item that is currently not present in the collection;
      // 2. the field may be 'locked', because it was changed on user's machine, and
      // this change was not yet confirmed from server
      if (record != null && subscription._modifiedItems.containsKey(record['_id'])) {
        collectRes = false;
        logger.fine('discarding diff');
      }
       if (record != null && !subscription._modifiedItems.containsKey(record['_id'])) {
        logger.fine('aplying changes!');
        if (collectRes) res = max(res, change['version']);
        applyChange(change["data"], record, 'clean_sync');
      }
    }
      else if (change["action"] == "remove" ) {
      logger.fine('aplying changes!');
      if (collectRes) res = max(res, change['version']);
      collection.removeWhere((d) => d["_id"] == change["_id"], author: 'clean_sync');
    }
    logger.finest('applying finished: $subscription ${subscription.collection} ${subscription._version}');
  });
  return res;
}

class Subscription {
  // constructor arguments:
  String collectionName;
  DataSet collection;
  Connection _connection;
  bool lock = false;
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
  get version => _version;

  Completer _initialSync = new Completer();
  List<StreamSubscription> _subscriptions = [];
  StreamController errorStreamController;

  /// Completes after first request to get data is answered and handled.
  Future get initialSync => _initialSync.future;

  Subscription.config(this.collectionName, this.collection, this._connection,
      this._author, this._idGenerator, this._handleData, this._handleDiff,
      this._forceDataRequesting, [this.args]);

  Subscription(this.collectionName, this._connection, this._author,
      this._idGenerator, [this.args]) {
    collection = new DataSet();
    collection.addIndex(['_id']);
    errorStreamController = new StreamController.broadcast();
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
        }
      });
    }

    var change = new ChangeSet();

    notify(){
      new Timer(new Duration(), (){
        change.addedItems.forEach((data) {
          Future result = _connection.send(() => new ClientRequest("sync", {
            "action" : "add",
            "collection" : collectionName,
            "data" : cleanify(decleanify(data)),
            "author" : _author
          })).then((result) {
            if (result['error'] != null)
              errorStreamController.add(result['error']);
          });
          markToken(data['_id'], result);
        });

        change.strictlyChanged.forEach((DataMap data, ChangeSet changeSet) {
          Future result = _connection.send(() => new ClientRequest("sync", {
            "action" : "change",
            "collection" : collectionName,
            "_id": data["_id"],
            "change" : cleanify(decleanify(data)),
            "author" : _author
          })).then((result) {
            if (result['error'] != null)
              errorStreamController.add(result['error']);
          });
          // TODO: check if server really accepted the change
          markToken(data['_id'], result);
        });

        change.removedItems.forEach((data) {
          Future result = _connection.send(() => new ClientRequest("sync", {
            "action" : "remove",
            "collection" : collectionName,
            "_id" : data["_id"],
            "author" : _author
          })).then((result) {
            if (result['error'] != null)
              errorStreamController.add(result['error']);
          });
          markToken(data['_id'], result);
        });
        change = new ChangeSet();
      });
    }

    _subscriptions.add(collection.onChangeSync.listen((event) {
      if (event["author"] != 'clean_sync') {
        var newChange = event['change'];
        assert(newChange is ChangeSet);
        change.mergeIn(newChange);
        notify();
      }
    }));
  }

  _createDataRequest() => new ClientRequest("sync", {
    "action" : "get_data",
    "collection" : collectionName
  });

  _createDiffRequest() {
    if (lock) {
      return null;
    } else {
      lock = true;
      return new ClientRequest("sync", {
      "action" : "get_diff",
      "collection" : collectionName,
      "version" : _version
      });
    }
  }

  // TODO rename to something private-like
  void setupDataRequesting() {
    // request initial data
    _connection.send(_createDataRequest).then((response) {
      if (response['error'] != null) {
        if (!_initialSync.isCompleted) _initialSync.completeError(new ArgumentError(response['error']));
        else throw new ArgumentError(response['error'].toString());
        return;
      }
      _version = response['version'];
      _handleData(response['data'], collection, _author);

      logger.log(logger.level, "Test message");
      logger.info("Got initial data, synced to version ${_version}");

      // TODO remove the check? (restart/dispose should to sth about initialSynd)
      if (!_initialSync.isCompleted) _initialSync.complete();

      var subscription = _connection
        .sendPeriodically(_forceDataRequesting ?
            _createDataRequest : _createDiffRequest)
        .listen((response) {
          lock = false;
          // id data and version was sent, diff is set to null
          if(response['diff'] == null) {
            _version = response['version'];
            _handleData(response['data'], collection, _author);
          } else {
            if(!response['diff'].isEmpty) {
              _version = max(_version, _handleDiff(response['diff'], this, _author));
              }
            }
        }, onError: (e){if (e is! CancelError)throw e;});

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

  Future close() {
    dispose();
    return Future.wait(_modifiedItems.values);
  }

  void restart() {
    dispose();
    start();
  }

  Stream onClose() {

  }
}
