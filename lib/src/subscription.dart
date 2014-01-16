// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

void handleData(List<Map> data, DataCollection collection, String author) {
  collection.clear(author: author);
  List<Data> toAdd = [];
  for (Map record in data) {
    toAdd.add(new Data.from(record));
  }
  collection.addAll(toAdd, author: author);
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
  Communicator _communicator;
  String _author;
  IdGenerator _idGenerator;
  Map args = {};
  Future get initialSync => _communicator._initialSync.future;

  List<StreamSubscription> _subscriptions = [];

  Subscription.config(this.collectionName, this.collection, this._connection,
      this._communicator, this._author, this._idGenerator, [this.args]);

  Subscription(this.collectionName, this._connection, this._author,
      this._idGenerator, [this.args]) {
    collection = new DataCollection();
    _communicator = new Communicator(_connection, collectionName,
        (List<Map> data) {handleData(data, collection, _author);},
        (List<Map> diff) {handleDiff(diff, collection, _author);});
    start();
  }

  /**
   * Waits for initialSync of all provided subscriptions.
   */
  static Future wait(List<Subscription> subscriptions) {
    return Future.wait(
        subscriptions.map((subscription) => subscription.initialSync));
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

  void start() {
    _setupListeners();
    _communicator.start();
  }

  void dispose() {
    _communicator.stop();
    _subscriptions.forEach((s) {s.cancel();});
  }

  void restart() {
    dispose();
    start();
  }

  Stream onClose() {

  }
}
