// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

class Subscription {
  String collectionName;
  DataCollection collection;
  Connection _connection;
  Communicator _communicator;
  String _author;
  IdGenerator _idGenerator;
  Map args = {};
  List<StreamSubscription> _subscriptions = [];

  Subscription.config(this.collectionName, this.collection, this._connection,
      this._communicator, this._author, this._idGenerator, [this.args]);

  Subscription(this.collectionName, this._connection, this._author,
      this._idGenerator, [this.args]) {
    collection = new DataCollection();
    _communicator = new Communicator(_connection, collectionName,
        this.handleData, this.handleDiff);
  }

  void _setupListeners() {
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
    }));
  }

  void start() {
    _setupListeners();
    _communicator.start();
  }

  void dispose() {
    _communicator.stop();
    _subscriptions.forEach((s) {s.cancel();});
  }

  void handleData(List<Map> data) {
    // TODO: use clean(author: _author instead)
    var toDelete=[];
    for (var d in collection) {
      toDelete.add(d);
    }
    for (var d in toDelete) {
      collection.remove(d, author: _author);
    }
    for (Map record in data) {
      this.collection.add(new Data.from(record), author : _author);
    }
  }

  void handleDiff(List<Map> diff) {
    diff.forEach((change) {
      _applyChange(change);
    });
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
  }

  Stream onClose() {

  }
}
