// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

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

final Logger _logger = new Logger('clean_sync.subscription');

/// Applies [data] (which is a response from server to get_data request) to
/// [subscription], so that the subsribed-to collection is in sync with
/// received data.
///
/// This function is not intended for direct use in production (it is public
/// only for testing purposes).
void handleData(List<Map> data, Subscription subscription) {
  _logger.fine('handleData for ${subscription.resourceName}: ${data}');
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

/// Applies all necessary changes to [target] so that it is in accordance with
/// [source].
///
/// This function is not intended for direct use in production (it is public
/// only for testing purposes).
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

/// Applies [diff] (which is a response from server to get_diff request) to
/// [subscription], so that the subsribed-to collection is in sync with
/// received data.
///
/// This function is not intended for direct use in production (it is public
/// only for testing purposes).
num handleDiff(List<Map> diff, Subscription subscription) {
  _logger.fine('handleDiff: subscription: $subscription'
              'diffSize: ${diff.length}, diff: $diff');

  subscription.updateLock.value = true;
  DataSet collection = subscription.collection;
  var version = subscription._version;
  num res = -1;

  try {
    diff.forEach((Map change) {
      var _records = collection.findBy("_id", change["_id"]);
      if (_records.length > 1) {
        throw new Exception('id in the coll is not unique ${_records}');
      }
      DataMap record = _records.isNotEmpty? _records.first : null;
      String action = change["action"];


      _logger.finer('handling change $change');
  //     it can happen, that we get too old changes
      if (!change.containsKey('version')){
        _logger.warning('change does not contain "version" field. If not testing, '
                       'this is probably bug. (change: $change)');
        change['version'] = 0;
      } else if (version == null) {
        _logger.warning('Subscription $subscription version is null. If not testing, '
                       'this is probably bug.');
      } else if(change['version'] <= version) {
        return;
      }
      if (action == "add") {
        res = max(res, change['version']);
        if (record == null) {
          _logger.finer('aplying changes (add)');
          collection.add(change["data"]);
        } else {
          _logger.finer('id already present and add should be applied => '
                       'applying changes (add)');
          applyChange(change["data"], record);
        }
      }
      else if (action == "change" ) {
        // TODO check if the record is not currently participating in some running operation
        // would be nice although it is not necessary
        if (record != null) {
           _logger.finer('aplying changes (change)');
           res = max(res, change['version']);
           applyChange(change["data"], record);
        }
      }
      else if (action == "remove" ) {
        _logger.finer('applying changes (remove');
        res = max(res, change['version']);
        collection.remove(record);
      }
      _logger.finest('applying finished: $subscription ${subscription.collection} ${subscription._version}');
    });
  } catch (e) {
    if (e is Exception) {
      subscription.updateLock.value = false;
      throw e;
    }
  }
  _logger.fine('handleDiff ends');
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
  TransactorClient transactor;
  final Function _handleData;
  final Function _handleDiff;
  // Used for testing and debugging. If true, data (instead of diff) is
  // requested periodically.
  bool _forceDataRequesting = false;
  Map args = {};
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

  /// Used in experimental feature "connection recovery".
  Stream get onResyncFinished => _onResyncFinishedController.stream;

  /// Used in experimental feature "connection recovery".
  Stream get onFullSync => _onFullSyncController.stream;

  /// Returns [true] if [dispose] has been called on this.
  bool get disposed => collection == null;


  /// version is exposed only for testing and debugging
  get version => _version;

  /// Returns string representation of this [Subscription].
  String toString() => 'Subscription(ver: ${_version})';

  Completer _initialSync;
  List<StreamSubscription> _subscriptions = [];
  StreamController _errorStreamController = new StreamController.broadcast();
  StreamSubscription _periodicDiffRequesting;

  /// Returns a stream of errors that might arise during operation of this
  /// [Subscription].
  Stream get errorStream {
    if (!_initialSync.isCompleted) throw new StateError("Initial sync not complete yet!");
    return _errorStreamController.stream;
  }

  /// Completes after first request to get data is answered and handled.
  Future get initialSync => _initialSync.future;
  get initialSyncCompleted => _initialSync.isCompleted;

  static _createNewCollection() {
    var collection = new DataSet();
    collection.addIndex(['_id']);
    return collection;
  }

  /// Creates a [Subscription], allowing to specify more parameters than the
  /// other [Subscription] constructor. This should not be called to create
  /// subscriptions in production code, one should rather use
  /// [Subscriber.subscribe] for this purpose.
  ///
  /// [collection] is the initial collection of data for this [Subscription] to
  /// start with.
  /// [_handleData] and [_handleDiff] may be overriden for testing purposes.
  /// [_forceDataRequesting] set to [true] would make the subscription to
  /// send only get_data requests (instead of get_diff requests, that are
  /// usually sufficient), which may be useful for testing purposes.
  /// Meaning of other arguments is explained in the doc of the other
  /// [Subscription] constructor.
  Subscription.config(this.resourceName, this.mongoCollectionName, this.collection,
      this._connection, this._idGenerator, this.transactor, this._handleData,
      this._handleDiff, this._forceDataRequesting, this.updateLock) {
    _initialSync = createInitialSync();
  }

  /// Creates a [Subscription]. This should not be called to create
  /// subscriptions in production code, one should rather use
  /// [Subscriber.subscribe] for this purpose.
  ///
  /// [resourceName] is the name of resource, that is, the name under which
  /// the resource is published on server.
  /// [mongoCollectionName] is the name of mongo collection on which the
  /// resource is based. (In case there is no [MongoProvider] on the server
  /// side but some other [DataProvider], this may be null.)
  /// [TransactorClient] is client-side endpoint for submitting transactions.
  Subscription(String resourceName, String mongoCollectionName,
               Connection connection, IdGenerator idGenerator,
               TransactorClient transactor, DataReference<bool> updateLock)
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
    _logger.info("Resyncing subscription ${this}");
    if (!_initialSync.isCompleted) {
      _logger.finer("Initial sync is not completed, restarting");
      restart();
      return;
    }
    List<Future> actions = [];

    if (_periodicDiffRequesting.isPaused) {
      _logger.fine("Resuming periodic diff requesting");
      _periodicDiffRequesting.resume();
    }

    Future.wait(actions).then((_) {
      _onResyncFinishedController.add(null);
    });
  }

  /// Used in experimental feature "connection recovery".
  void setupConnectionRecovery() {
    _logger.info("Setting up connection recovery for ${this}");
    _subscriptions.add(_connection.onDisconnected.listen((_) {
      _connected = false;
    }));

    _subscriptions.add(_connection.onConnected.listen((_) {
      _connected = true;
      _resync();
    }));
  }


  /// This method is public only for testing purposes.
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
          operation = () => transactor.performServerOperation('addAll',
            {'data': new List.from(change.addedItems)},
            subs: [this]
          );
        } else if (change.removedItems.length > 0) {
          assert(change.removedItems.length == 1);
          operation = () => transactor.performServerOperation('removeAll',
            {"ids" : new List.from(change.removedItems.map((e) => e['_id']))},
            subs: [this]
          );
        } else {
          // Only one item should be changed
          assert(change.changedItems.length == 1);
          operation = () => transactor.performServerOperation("change",
            change.changedItems.values.first.toJson(),
            docs: [change.changedItems.keys.first]
          );
        }
        Future result;
        transactor.operationPerformed = true;
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
    _logger.finer("${this} sending data request with args ${args}");

    return new ClientRequest("sync", {
      "action" : "get_data",
      "collection" : resourceName,
      'args': args
    });
  }

  _createDiffRequest() {
    _logger.finest("${this} entering createDiffRequest");
    if (requestLock || _sentItems.isNotEmpty) {
      return null;
    } else {
      _logger.finest("${this} sending diff request with args ${args}");
      requestLock = true;
      transactor.operationPerformed = false;

      return new ClientRequest("sync", {
        "action" : "get_diff",
        "collection" : resourceName,
        'args': args,
        "version" : _version
      });
    }
  }

  Future _setupDataRequesting({initialData}) {
    // request initial data; this is also called when restarting subscription
    _logger.info("Setting up data requesting for ${this}");
    /// remember initialSync, that was active in thsi moment. Later, when we get
    /// initial data, we check, if this completer is already completed - if so,
    /// it means, the subscription was restarted sooner than initialSync-ed
    var oldInitialSync =_initialSync;
    Future data;

    if(initialData != null && initialData['data'] != null) {
      _logger.fine('Initiating ${this.resourceName} with data $initialData');
      data = new Future.value(initialData);
    }
    else {
      data = _connection.send(_createDataRequest);
      _logger.fine('Initiating ${this.resourceName} without data');
    }


    return data.then((response) {
      if (oldInitialSync.isCompleted) {
        return;
      }
      if (response['error'] != null) {
        _logger.warning("Response to 'send' completed with error ${response['error']}");
        if (!_initialSync.isCompleted) _initialSync.completeError(new DatabaseAccessError(response['error']));
        else _errorStreamController.add(new DatabaseAccessError(response['error']));
        return;
      }
      _version = response['version'];
     _handleData(response['data'], this);
      _connected = true;

      _logger.info("Got initial data for $resourceName, synced to version ${_version}");

      // TODO remove the check? (restart/dispose should to sth about initialSynd)
      if (!_initialSync.isCompleted) _initialSync.complete();

      _setupPeriodicDiffRequesting();
    });
  }

  void _setupPeriodicDiffRequesting() {
    _logger.info("Setting up periodic diff requesting for ${this}");
    _periodicDiffRequesting = _connection
        .sendPeriodically(_forceDataRequesting ?
            _createDataRequest : _createDiffRequest)
        .listen((response) {
            requestLock = false;
            if(transactor.operationPerformed == true) {
              return false;
            }

            // id data and version was sent, diff is set to null
            if (response['error'] != null) {
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
          requestLock = false;
          if (e is CancelError) { /* do nothing */ }
          else if (e is ConnectionError) {
            // connection failed
            _periodicDiffRequesting.pause();
          }
          else {
            _logger.shout('', e, s);
            throw e;
          }
        });
    _subscriptions.add(_periodicDiffRequesting);
  }

  void _start({initialData}) {
    _started = true;
    _logger.info("${this} starting");
    _errorStreamController.stream.listen((error){
      if(!error.toString().contains("__TEST__")) {
        _logger.shout('errorStreamController error: ${error}');
      }
    });
    setupConnectionRecovery();
    setupListeners();
    _setupDataRequesting(initialData: initialData);
  }


  Future _closeSubs() {
    _logger.info("Closing all stream subscriptions of ${this}");
    List subToClose = [];
    _subscriptions.forEach((sub) => subToClose.add(sub.cancel()));
    _subscriptions.clear();
    subToClose.retainWhere((e) => e != null);

    return Future.wait(subToClose).then(
        (_) => Future.wait(_sentItems));
  }

  /// Stops syncing [collection] with the published resource in both ways. That
  /// is, after calling [dispose], new changes on server will not be requested
  /// by this [Subscription] and changes made on this client will not be
  /// propagated to server. The value of [collection] is set to null.
  Future dispose(){
    _logger.info("Disposing of ${this}");
    if (!_initialSync.isCompleted) _initialSync.completeError(new CanceledException());
    return _closeSubs()
      .then((_) {
        // check to make multiple disposes safe
        if (collection != null)
          collection.dispose();
        collection = null;
      });
  }

  /// Restarts the syncing of [collection], or starts syncing if it was not
  /// started. [restart] may be used to change the [args]. The value of [args]
  /// is propagated to server and it is be passed to [DataGenerator] to create
  /// the desired [DataProvider].
  ///
  /// After [restart] is called, the changes to this resource on server will be
  /// periodically requested by this [Subscription], received and applied to the
  /// [collection]. Also, all changes of the collection made on this client
  /// will be listened to and propagated to server.
  ///
  /// If [initialData] is null, get_data request is first sent to server
  /// to initialize the [collection]. Alternatively, if [initialData] is
  /// provided it is used to initialize the collection and get_data request is
  /// not sent. In both cases, the subscription continues with get_diff requests
  /// after the [collection] has been initialized.
  void restart({Map args: const {}, initialData}) {
    _logger.info("Restarting ${this} with args: ${args}");
    this.args = args;
    if (!_started) {
      _logger.fine("First restart of subscription");
      _start(initialData: initialData);
    } else {
      if (!_initialSync.isCompleted) _initialSync.completeError(new CanceledException());
      _initialSync = createInitialSync();
      _closeSubs().then((_) {
        requestLock = false;
        if(collection == null)
          collection = _createNewCollection();
        _start(initialData: initialData);
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
