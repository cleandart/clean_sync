// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

class Subscription {

  Connection _connection;
  Timer _timer;
  String collectionName;
  IdGenerator _idGenerator;
  DataCollection collection;
  Map args = {};
  num _version = -1;
  String _author;

  Subscription(this.collectionName, this._connection, this._author,
      this._idGenerator, [this.args]) {
    collection = new DataCollection();
    _setupListeners();
    _requestInitialData().then((_) => _setupDiffPolling());
  }

  void _setupListeners() {
    collection.onBeforeAdd.listen((data) {
      // if data["_id"] is null, it was added by this client and _id should be
      // assigned
      if(data["_id"] == null) {
        data["_id"] = _idGenerator.next();
      }
    });

    collection.onChangeSync.listen((event) {
      if (event["author"] == null) {
        event["change"].addedItems.forEach((data) {
          _connection.sendRequest(() => new ClientRequest("sync", {
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

          _connection.sendRequest(() => new ClientRequest("sync", {
            "action" : "change",
            "collection" : collectionName,
            "_id" : data["_id"],
            "data" : change,
            "author" : _author
          }));
        });

        event["change"].removedItems.forEach((data) {
          _connection.sendRequest(() => new ClientRequest("sync", {
            "action" : "remove",
            "collection" : collectionName,
            "_id" : data["_id"],
            "author" : _author
          }));
        });
      }
    });
  }

  Future _requestInitialData() {
    return _connection.sendRequest(() => new ClientRequest("sync", {
      "action" : "get_data",
      "collection" : collectionName
    })).then((response) {
      _handleResponse(response);
      print("Got initial data, synced to version ${_version}");
      return _version;
    });
  }

  void _setupDiffPolling() {
    _timer = new Timer.periodic(new Duration(seconds: 2), (_) {
      _connection.sendRequest(() => new ClientRequest("sync", {
        "action" : "get_diff",
        "collection" : collectionName,
        "version" : _version
      })).then((response) {
        _handleResponse(response);
      });
    });
  }

  void _handleResponse(Map response) {
    // TODO: use clean(author: _author instead)
    if(response.containsKey('data')) {
      var toDelete=[];
      for (var d in collection) {
        toDelete.add(d);
      }
      for (var d in toDelete) {
        collection.remove(d, author: _author);
      }
      for (Map record in response['data']) {
        this.collection.add(new Data.from(record), author : _author);
      }
      _version = response['version'];
    } else if(response.containsKey('diff') && response['diff'] != null) {
      response['diff'].forEach((change) {
        _applyChange(change);
      });
    }
  }

  void _applyChange(Map change) {
    if (change["author"] != _author) {
      if (change["action"] == "add") {
        collection.add(new Data.from(change["data"]), author: _author);
      }
      else if (change["action"] == "change") {
        Data record = collection.firstWhere((d) => d["_id"] == change["_id"]);
        if (record != null) {
          record.addAll(change["data"], author: _author);
        }
      }
      else if (change["action"] == "remove") {
        Data record = collection.firstWhere((d) => d["_id"] == change["_id"]);
        if (record != null) {
          collection.remove(record, author: _author);
        }
      }
    }
    print("applying: ${change}");
    _version = change["version"];
  }

  void close() {
    _timer.cancel();
  }

  Stream onClose() {

  }
}
