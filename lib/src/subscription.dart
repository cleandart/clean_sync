// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

void handleData(List<Map> data, DataCollection collection, String author) {
  // TODO: use clean(author: _author instead)
  var toDelete=[];
  for (var d in collection) {
    toDelete.add(d);
  }
  for (var d in toDelete) {
    collection.remove(d, author: author);
  }
  for (Map record in data) {
    collection.add(new Data.from(record), author : author);
  }
}

void handleDiff(List<Map> diff, DataCollection collection, String author) {
  var profiling = new Stopwatch()..start();
  diff.forEach((Map change) {
    if (change["author"] != author) {
      if (change["action"] == "add") {
        collection.add(new Data.from(change["data"]), author: author);
      }
      else if (change["action"] == "change") {
        Data record = collection.firstWhere((d) => d["_id"] == change["_id"]);
        if (record != null) {
          record.addAll(change["data"], author: author);
        }
      }
      else if (change["action"] == "remove") {
        Data record = collection.firstWhere((d) => d["_id"] == change["_id"],
            orElse: ()=>null);
        if(record == null) {
          var collectionMap = collection.toList().map((DataView d) => d.toJson());
          throw new Exception(
              'cannot find obj with id ${change["_id"]} in $collectionMap');
        }
        //TODO: can result be null?
        if (record != null) {
          collection.remove(record, author: author);
        }
      }
    }
    print("handleDiff:${profiling.elapsed}");
    profiling.stop();
    print("applying: ${change}");
  });
}

class Subscription {
  String collectionName;
  DataCollection collection;
  Connection _connection;
  String _author;
  IdGenerator _idGenerator;
  Map args = {};
  String _updateStyle;
  num _version;
  Function _handleData, _handleDiff;
  StreamSubscription _updateSubscription;
  Completer _initialSync = new Completer();
  Future get initialSync => _initialSync.future;

  List<StreamSubscription> _subscriptions = [];

  Subscription.config(this.collectionName, this.collection, this._connection,
      this._author, this._idGenerator, this._handleData, this._handleDiff, 
      this._updateStyle, [this.args]
  ) {
    
  }

  Subscription(this.collectionName, this._connection, this._author,
      this._idGenerator, [this.args]) {
    collection = new DataCollection();
    
    _handleData = handleData;
    _handleDiff = handleDiff;
    _updateStyle = 'diff';
    
    start();
  }

  /**
   * Waits for initialSync of all provided subscriptions.
   */
  static Future wait(List<Subscription> subscriptions) {
    return Future.wait(
        subscriptions.map((subscription) => subscription.initialSync));
  }

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

        event["change"].changedItems.forEach((Data data, ChangeSet changeSet) {
          Map change = {};
          changeSet.changedItems.
            forEach((k, Change v) => change[k] = v.newValue);

          _connection.send(() => new ClientRequest("sync", {
            "action" : "change",
            "collection" : collectionName,
            "_id": data["_id"],
            "change" : change,
            "author" : _author
          }));
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
  
  void setupDataRequesting() {
    // request initial data
    _connection.send(() => new ClientRequest("sync", {
      "action" : "get_data",
      "collection" : collectionName
    })).then((response) {
      _version = response['version'];
      _handleData(response['data'], collection, _author);
      
      print("Got initial data, synced to version ${_version}");
      
      if (!_initialSync.isCompleted) {
        _initialSync.complete();
      }
      
      if (_updateStyle == 'diff') {
        _requestDiff();
      } else if (_updateStyle == 'data') {
        _requestData();
      }

    });
  }
  
  void start() {
    setupListeners();
    setupDataRequesting();
  }
  
  void _requestDiff() {
    _updateSubscription = _connection.sendPeriodically(() => new ClientRequest("sync", {
      "action" : "get_diff",
      "collection" : collectionName,
      "version" : _version
    })).listen((response) {
      // id data and version was sent, diff is set to null
      if(response['diff'] == null) {
        _version = response['version'];
        _handleData(response['data'], collection, _author);
      } else {
        if(!response['diff'].isEmpty) {
          _version = response['diff'].map((item) => item['version'])
              .reduce(max);
          _handleDiff(response['diff'], collection, _author);
        }
      }
    });
  }
  
  void _requestData() {
    _updateSubscription = _connection.sendPeriodically(() => 
        new ClientRequest("sync", {
          "action" : "get_data",
          "collection" : collectionName
        })
    ).listen((response) {
      _version = response['version'];
      _handleData(response['data'], collection, _author);
    });
  }

  void dispose() {
    _updateSubscription.cancel();
    _subscriptions.forEach((s) {s.cancel();});
  }

  void restart() {
    dispose();
    start();
  }

  Stream onClose() {

  }
}
