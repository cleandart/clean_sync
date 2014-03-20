// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

emptyStartup(_){}

/**
 * When subscription is disposed sooner than initialSync is completed, initialSync
 * completes with error (with CancelError). Programmers
 * very seldom want to react to this error, since in most cases, you can silently
 * ignore it. Therefore, it's better to catch it by default.
 */
Completer createInitialSync(){
  var res = new Completer();
  res.future.catchError((e){});
  return res;
}

final Logger logger = new Logger('clean_sync.subscription');

void handleData(List<Map> data, Subscription subscription, String author) {
  logger.fine('handleData: ${data}');
  var collection = subscription.collection;
  subscription.updateLock = true;
  collection.clear();
  collection.addAll(data);
  subscription.updateLock = false;
}

void _applyChangeList (List source, DataList target) {
  target.length = source.length;
  for (num i=0; i<target.length; i++) {
    if (!applyChange(source[i], target[i])) {
      target.set(i, source[i]);
    }
  }
}

void _applyChangeMap (Map source, DataMap target) {
  for (var key in new List.from(source.keys)) {
    if (target.containsKey(key)) {
      if(!applyChange(source[key], target[key])){
        target.add(key, source[key]);
      }
    } else {
      target.add(key, source[key]);
    }
  }
  for (var key in new List.from(target.keys)) {
    if (!source.containsKey(key)) {
      target.remove(key);
    }
  }
}

bool applyChange (source, target) {
  if (source is Map && target is Map) {
    _applyChangeMap(source, target);
    return true;
  }
  if (source is List && target is List) {
    _applyChangeList(source, target);
    return true;
  }
  if(source == target) {
    return true;
  }
  return false;
}

void destroyMap(Map m) {
  m.forEach((k,v){
    destroyStructure(v);
  });
  for (var k in new List.from(m.keys)) {
    m.remove(k);
  }
}

void destroyIterable(var l) {
  l.forEach((v){
    destroyStructure(v);
  });
  for (var v in new List.from(l)) {
    l.remove(v);
  }

}


void destroyStructure(s){
  if (s is Map) {
    destroyMap(s);
  } else
  if (s is Iterable) {
    destroyIterable(s);
  } else {}

}

num handleDiff(List<Map> diff, Subscription subscription, String author) {
  logger.fine('handleDiff: subscription: $subscription, author: $author,'
              'diffSize: ${diff.length}, diff: $diff');
  subscription.updateLock = true;
  DataSet collection = subscription.collection;
  var version = subscription._version;
  num res = -1;

  try {
    diff.forEach((Map change) {
      var _records = collection.findBy("_id", change["_id"]);
      DataMap record = _records.isNotEmpty? _records.first : null;
      String action = change["action"];

      logger.finer('handling change $change');
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
      if (action == "add") {
        res = max(res, change['version']);
        if (record == null) {
          logger.finer('aplying changes (add)');
          collection.add(change["data"]);
        } else {
          logger.finer('add discarded; same id already present');
          assert(author == change['author']);
        }
      }
      else if (action == "change" ) {
        // 1. the change may be for item that is currently not present in the collection;
        // 2. the field may be 'locked', because it was changed on user's machine, and
        // this change was not yet confirmed from server
         if (record != null) {
           if(!subscription._sentItems.containsKey(record['_id'])
             && !subscription._modifiedItems.changedItems.containsKey('_id')) {
              logger.finer('aplying changes (change)');
              res = max(res, change['version']);
              applyChange(change["data"], record);
           } else {
             logger.finer('discarding diff');
             throw "stop";
           }
        }
      }
      else if (action == "remove" ) {
        logger.finer('applying changes (remove');
        res = max(res, change['version']);
        collection.remove(record);
      }
      logger.finest('applying finished: $subscription ${subscription.collection} ${subscription._version}');
    });
  } catch (e) {
    if (e is Exception) {
      throw e;
    }
  }
  logger.fine('handleDiff ends');
//  destroyStructure(diff);
  subscription.updateLock = false;
  return res;
}

class CanceledException implements Exception {
  String toString() {
    return "CancelException";
  }
}

class Subscription {
  // constructor arguments:
  String collectionName;
  DataSet collection;
  Connection _connection;
  // author field is not used anymore; we are keeping it in the DB mainly for debugging
  // and logging purposes
  String _author;
  IdGenerator _idGenerator;
  final Function _handleData;
  final Function _handleDiff;
  // Used for testing and debugging. If true, data (instead of diff) is
  // requested periodically.
  bool _forceDataRequesting = false;
  Map args = {};
  // Maps _id of a document to a structure holding the document at the time of sending
  // along with client version of the change and failed flag
  Map<String, Map<String, dynamic>> _sentItems = {};
  // reflects changes to this.collection, that were not already sent to the server
  ChangeSet _modifiedItems = new ChangeSet();
  // flag used to prevent subscription to have multiple get_diff requests 'pending'.
  // This is mainly solved by clean_ajax itself; however, following is still possible:
  // 1. send_diff
  // 2. response obtained, response listener notified, end
  // 3. send_diff
  // 4. response listener process diff requested in step 1.
  // clearly, send_diff in step 3 can and should be avoided.
  bool requestLock = false;
  // this is another approach to obtain functionality formerly provided by clean_data
  // authors; when applying changes obtained from server, use this flag to
  // prevent detection and re-sending of these changes to the server
  bool updateLock = false;
  // all changes with version < _version MUST be already applied by this subscription.
  // Some of the later changes may also be applied; this happens, when collection
  // applies user change, but is not synced to the very last version at that moment.
  num _version = 0;

  bool _connected = true;

  StreamController _onResyncFinishedController = new StreamController.broadcast();
  StreamController _onFullSyncController = new StreamController.broadcast();

  Stream get onResyncFinished => _onResyncFinishedController.stream;
  Stream get onFullSync => _onFullSyncController.stream;


  // version exposed only for testing and debugging
  get version => _version;

  String toString() => 'Subscription(${_author}, ver: ${_version})';
  Completer _initialSync;
  List<StreamSubscription> _subscriptions = [];
  StreamController _errorStreamController = new StreamController.broadcast();
  StreamSubscription _periodicDiffRequesting;
  Stream get errorStream {
    if (!_initialSync.isCompleted) throw new StateError("Initial sync not complete yet!");
    return _errorStreamController.stream;
  }

  /// Completes after first request to get data is answered and handled.
  Future get initialSync => _initialSync.future;

  static _createNewCollection() {
    var collection = new DataSet();
    collection.addIndex(['_id']);
    return collection;
  }

  Subscription.config(this.collectionName, this.collection, this._connection,
      this._author, this._idGenerator, this._handleData, this._handleDiff,
      this._forceDataRequesting, [this.args, startup = emptyStartup]) {
    _initialSync = createInitialSync();
    startup(this);
  }

  Subscription(collectionName, connection, author, idGenerator, [args])
      : this.config(collectionName, _createNewCollection(), connection, author,
          idGenerator, handleData, handleDiff, false, args, (self) => self.start());


  /**
   * Waits for initialSync of all provided subscriptions.
   */
  static Future wait(List<Subscription> subscriptions) {
    return Future.wait(
        subscriptions.map((subscription) => subscription.initialSync));
  }

  void _resync() {
    List<Future> actions = [];

    // resend all failed changes
    _sentItems.forEach((id, item) {
      if (item["failed"]) {
        actions.add(_send(id, item["data"]));
      }
    });

    if (!this.updateLock) {
      for (var key in new List.from(_modifiedItems.changedItems.keys)) {
        if (!_sentItems.containsKey(key['_id'])) {
          _sendRequest(key);
        }
      }
    }

    if (_periodicDiffRequesting.isPaused) {
      _periodicDiffRequesting.resume();
    }

    Future.wait(actions).then((_) {
      _onResyncFinishedController.add(null);
    });
  }

  void setupConnectionRecovery() {
    _connection.onDisconnected.listen((_) {
      _connected = false;
    });

    _connection.onConnected.listen((_) {
      _connected = true;
      _resync();
    });
  }

  void _sendRequest(DataMap elem) {
    assert(_modifiedItems.changedItems.containsKey(elem));

    if (_connected) {
      Map data;
      String clientVersion = _idGenerator.next();

      if (_modifiedItems.addedItems.contains(elem)) {
        data = {
          "action" : "add",
          "collection" : collectionName,
          "data" : new DataMap.from(elem),
          'args': args,
          "author" : _author,
          "clientVersion" : clientVersion
        };
      }
      if (_modifiedItems.strictlyChanged.containsKey(elem)) {
        data = {
          "action" : "change",
          "collection" : collectionName,
          'args': args,
          "_id" : elem["_id"],
          "change" : new DataMap.from(elem),
          "author" : _author,
          "clientVersion" : clientVersion
        };
      }
      if (_modifiedItems.removedItems.contains(elem)) {
        data = {
          "action" : "remove",
          "collection" : collectionName,
          'args': args,
          "_id" : elem["_id"],
          "author" : _author,
          "clientVersion" : clientVersion
        };
      }

      _send(elem["_id"], data);

      _modifiedItems.changedItems.remove(elem);
    }
  }

  Future _send(String id, Map data) {
    logger.finer('Sending #${id}, ${data}');

    Future result = _connection.send(() => new ClientRequest("sync", data))
      .then((result) {
        if (result is Map && result['error'] != null) {
          _errorStreamController.add(result['error']);
        }

        logger.finer('Sent #${id}, ${data}');

        _sentItems.remove(id);

        DataMap elem = _modifiedItems.changedItems.keys.firstWhere((e) => e["_id"] == id, orElse: () => null);

        // if there are some more changes, sent them
        if (elem != null){
          _sendRequest(elem);
        };

        if (_sentItems.isEmpty && _modifiedItems.changedItems.isEmpty) {
          _onFullSyncController.add(null);
        }

        return result;
      }, onError: (e) {
        if (e is ConnectionError) {
          _sentItems[id]["failed"] = true;
        }
        else if (e is CancelError) { /* do nothing */ }
        else throw e;
      });

    _sentItems[id] = {
      "data" : data,
      "failed" : false,
      "result" : result
    };

    return result;
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

    var change = new ChangeSet();

    _subscriptions.add(collection.onChangeSync.listen((event) {
      if (!this.updateLock) {
        ChangeSet change = event['change'];
        _modifiedItems.mergeIn(change);
        for (var key in change.changedItems.keys) {
          if (!_sentItems.containsKey(key['_id'])) {
            _sendRequest(key);
          }
        }
      }
    }));
  }

  _createDataRequest(){
    logger.finer("${this} sending data request with args ${args}");

    return new ClientRequest("sync", {
      "action" : "get_data",
      "collection" : collectionName,
      'args': args
    });
  }

  _createMaxClientVersionRequest() => new ClientRequest("sync", {
    "action" : "get_max_client_version",
    "collection" : collectionName,
    "author" : _author
  });

  _createDiffRequest() {
    logger.finest("${this} entering createDiffRequest");
    if (requestLock || _sentItems.isNotEmpty) {
      return null;
    } else {
      logger.finest("${this} sending diff request with args ${args}");
      requestLock = true;

      return new ClientRequest("sync", {
        "action" : "get_diff",
        "collection" : collectionName,
        'args': args,
        "version" : _version
      });
    }
  }

  void setupDataRequesting() {
    // request initial data; this is also called when restarting subscription
    _connection.send(_createDataRequest).then((response) {
      if (response['error'] != null) {
        if (!_initialSync.isCompleted) _initialSync.completeError(new DatabaseAccessError(response['error']));
        else _errorStreamController.add(new DatabaseAccessError(response['error']));
        return;
      }
      _version = response['version'];
      _handleData(response['data'], this, _author);
      _connected = true;

      logger.info("Got initial data, synced to version ${_version}");

      // TODO remove the check? (restart/dispose should to sth about initialSynd)
      if (!_initialSync.isCompleted) _initialSync.complete();

      _setupPeriodicDiffRequesting();
    });
  }

  void _setupPeriodicDiffRequesting() {
    _periodicDiffRequesting = _connection
        .sendPeriodically(_forceDataRequesting ?
            _createDataRequest : _createDiffRequest)
        .listen((response) {
          requestLock = false;
          // id data and version was sent, diff is set to null
          if (response['error'] != null) {
            throw new Exception(response['error']);
          }
          if(response['diff'] == null) {
            _version = response['version'];
            _handleData(response['data'], this, _author);
          } else {
            if(!response['diff'].isEmpty) {
              _version = max(_version, _handleDiff(response['diff'], this, _author));
            } else {
                if (response.containsKey('version'))
                   _version = response['version'];
            }
          }
        }, onError: (e){
          if (e is CancelError) { /* do nothing */ }
          else if (e is ConnectionError) {
            // connection failed
            _periodicDiffRequesting.pause();
            requestLock = false;
          }
          else throw e;
        });
    _subscriptions.add(_periodicDiffRequesting);
  }

  void start() {
    logger.info("${this} starting");
    _errorStreamController.stream.listen((error){
      if(!error.toString().contains("__TEST__")) {
        logger.shout('errorStreamController error: ${error}');
      }
    });
    setupConnectionRecovery();
    setupListeners();
    setupDataRequesting();
  }


  Future _closeSubs() {
    return Future.forEach(_subscriptions, (sub){
      sub.cancel();
    }).then((_) => Future.wait(_sentItems.values.map((item) => item["result"])));
  }

  Future dispose(){
    if (!_initialSync.isCompleted) _initialSync.completeError(new CanceledException());
    return _closeSubs()
      .then((_) => collection.dispose());
  }

  void restart([Map args]) {
    if (!_initialSync.isCompleted) _initialSync.completeError(new CanceledException());
    _initialSync = createInitialSync();
    this.args = args;
    _closeSubs().then((_) {
      requestLock = false;
      start();
    });
  }

  Stream onClose() {

  }
}

class DatabaseAccessError extends Error {
  final String message;
  DatabaseAccessError(this.message);
  String toString() => "Bad state: $message";
}
