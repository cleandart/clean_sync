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

void handleData(List<Map> data, Subscription subscription) {
  logger.fine('handleData: ${data}');
  var collection = subscription.collection;
  subscription.updateLock.value = true;
  collection.clear();
  collection.addAll(data);
  subscription.updateLock.value = false;
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

num handleDiff(List<Map> diff, Subscription subscription) {
  logger.fine('handleDiff: subscription: $subscription'
              'diffSize: ${diff.length}, diff: $diff');
  subscription.updateLock.value = true;
  DataSet collection = subscription.collection;
  var version = subscription._version;
  num res = -1;

//  if (diff.isNotEmpty) {
//    print(diff);
//  }


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
          print('diff ${subscription._transactor.author} add');
          collection.add(change["data"]);
        } else {
          logger.finer('add discarded; same id already present');
//          assert(author == change['author']);
        }
      }
      else if (action == "change" ) {
        // TODO check if the record is not currently participating in some running operation
        // would be nice although it is not necessary
        if (record != null) {


           logger.finer('aplying changes (change)');
           res = max(res, change['version']);
           applyChange(change["data"], record);

        }
      }
      else if (action == "remove" ) {
        print('diff ${subscription._transactor.author} remove');
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
  subscription.updateLock.value = false;
  return res;
}

class CanceledException implements Exception {
  String toString() {
    return "CancelException";
  }
}

class Subscription {
  // constructor arguments:
  String resourceName;
  String mongoCollectionName;
  DataSet collection;
  Connection _connection;
  Transactor _transactor;
  // author field is not used anymore; we are keeping it in the DB mainly for debugging
  // and logging purposes
//  String _author;
  final Function _handleData;
  final Function _handleDiff;
  // Used for testing and debugging. If true, data (instead of diff) is
  // requested periodically.
  bool _forceDataRequesting = false;
  Map args = {};
  // Maps _id of a document to a structure holding the document at the time of sending
  // along with client version of the change and failed flag. The structure of an
  // inner map is as: 'data' (DataMap), 'failed' (bool), 'result' (Future that completes
  // when request completes)
//  Map<String, Map<String, dynamic>> _sentItems = {};

  IdGenerator _idGenerator;
  Set _sentItems = new Set();
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
  DataReference<bool> updateLock;
  // all changes with version < _version MUST be already applied by this subscription.
  // Some of the later changes may also be applied; this happens, when collection
  // applies user change, but is not synced to the very last version at that moment.
  num _version = 0;


  bool _connected = true;

  bool _started = false;

  StreamController _onResyncFinishedController = new StreamController.broadcast();
  StreamController _onFullSyncController = new StreamController.broadcast();

  Stream get onResyncFinished => _onResyncFinishedController.stream;
  Stream get onFullSync => _onFullSyncController.stream;


  // version exposed only for testing and debugging
  get version => _version;

  String toString() => 'Subscription(ver: ${_version})';
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

  Subscription.config(this.resourceName, this.mongoCollectionName, this.collection,
      this._connection, this._idGenerator, this._transactor, this._handleData,
      this._handleDiff, this._forceDataRequesting, this.updateLock) {
    _initialSync = createInitialSync();
  }

  Subscription(resourceName, mongoCollectionName, connection, idGenerator,
               transactor, updateLock)
      : this.config(resourceName, mongoCollectionName, _createNewCollection(),
          connection, idGenerator, transactor, handleData, handleDiff, false,
          updateLock);


  /**
   * Waits for initialSync of all provided subscriptions.
   */
  static Future wait(List<Subscription> subscriptions) {
    return Future.wait(
        subscriptions.map((subscription) => subscription.initialSync));
  }

  //TODO MOVE resync to Transactor

  void _resync() {
    List<Future> actions = [];
    // resend all failed changes
//    _sentItems.forEach((id, item) {
//      if (item["failed"]) {
//        print('action ${id}');
//        //actions.add(_send(id, () => item["data"]));
//        actions.add(item["data"]());
//      }
//    });
//
//    Needs to be resolved for transactor
//
//    if (!this.updateLock) {
//      for (var key in new List.from(_modifiedItems.changedItems.keys)) {
//        if (!_sentItems.containsKey(key['_id'])) {
//          _sendRequest(key);
//        }
//      }
//    }

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


  void setupListeners() {
    var change = new ChangeSet();
    // TODO assign ID to document added
    _subscriptions.add(collection.onBeforeAdd.listen((dataObj) {
      assert(dataObj is Map);
      if (!dataObj.containsKey("_id")) dataObj["_id"] = _idGenerator.next();
      if (!dataObj.containsKey("__clean_collection")) dataObj["__clean_collection"] = mongoCollectionName;
    }));

    _subscriptions.add(collection.onChangeSync.listen((event) {
      if (this.updateLock.value == false) {
        ChangeSet change = event['change'];
        var operation;
        if (change.addedItems.length > 0) {
          operation = () => _transactor.performServerOperation('addAll',
            {'data': new List.from(change.addedItems)},
            colls: [[collection, mongoCollectionName]]
          );
        } else if (change.removedItems.length > 0) {
          assert(change.removedItems.length == 1);
          operation = () => _transactor.performServerOperation('removeAll',
            {"ids" : new List.from(change.removedItems.map((e) => e['_id']))},
            colls: [[collection, mongoCollectionName]]
          );
        } else {
          // Only one item should be changed
          assert(change.changedItems.length == 1);
          operation = () => _transactor.performServerOperation("change",
            change.changedItems.values.first.toJson(),
            docs: [change.changedItems.keys.first]
          );
        }
        Future result;
        result = operation()
          .then((res) {
            if (res is Map && res['result'] != null) res = res['result'];
            _sentItems.remove(result);
            return res;
          }, onError: (res){
            if (res is Map && res['error'] != null) res = res['error'];
            // Silent ignore - should be resolved
            _sentItems.remove(result);
            return res;
          });
        _sentItems.add(result);
      }
    }));
  }

  _createDataRequest(){
    logger.finer("${this} sending data request with args ${args}");

    return new ClientRequest("sync", {
      "action" : "get_data",
      "collection" : resourceName,
      'args': args
    });
  }

  _createDiffRequest() {
    logger.finest("${this} entering createDiffRequest");
    if (requestLock || _sentItems.isNotEmpty) {
      return null;
    } else {
      logger.finest("${this} sending diff request with args ${args}");
      requestLock = true;

      return new ClientRequest("sync", {
        "action" : "get_diff",
        "collection" : resourceName,
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
      _handleData(response['data'], this);
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
            print('tututu ${response}');
            throw new Exception(response['error']);
          }
          if(response['diff'] == null) {
            _version = response['version'];
            _handleData(response['data'], this);
          } else {
            if(!response['diff'].isEmpty) {
              _version = max(_version, _handleDiff(response['diff'], this));
            } else {
                if (response.containsKey('version'))
                   _version = response['version'];
            }
          }
        }, onError: (e, s){
          if (e is CancelError) { /* do nothing */ }
          else if (e is ConnectionError) {
            // connection failed
            _periodicDiffRequesting.pause();
            requestLock = false;
          }
          else {
            print('nunu');
            logger.shout('', e, s);
            print('nunu');
            throw e;
          }
        });
    _subscriptions.add(_periodicDiffRequesting);
  }

  void _start() {
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
    }).then((_) => Future.wait(_sentItems));
  }

  Future dispose(){
    if (!_initialSync.isCompleted) _initialSync.completeError(new CanceledException());
    return _closeSubs()
      .then((_) => collection.dispose());
  }

  void restart([Map args = const {}]) {
    this.args = args;
    if (!_started) {
      _started = true;
      _start();
    } else {
      if (!_initialSync.isCompleted) _initialSync.completeError(new CanceledException());
      _initialSync = createInitialSync();
      _closeSubs().then((_) {
        requestLock = false;
        _start();
      });
    }
  }

  Stream onClose() {

  }
}

class DatabaseAccessError extends Error {
  final String message;
  DatabaseAccessError(this.message);
  String toString() => "Bad state: $message";
}
