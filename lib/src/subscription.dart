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

void handleDiff(List<Map> diff, Subscription subscription, String author) {
  print(diff);
  DataCollection collection = subscription.collection;
  List<String> modifiedFields;
  
  diff.forEach((Map change) {
    if (change["action"] == "add") {
      Data record = collection.firstWhere((d) => d["_id"] == change["_id"], orElse : () => null);
      if (record == null) {
        collection.add(new Data.from(change["data"]), author: author);
      }
    }
    else if (change["action"] == "change" && change["author"] != author) {
      Data record = collection.firstWhere((d) => d["_id"] == change["_id"], orElse : () => null);
      if (record != null) {
        modifiedFields = subscription.modifiedDataFields(record);
        
        change["data"].forEach((String key, dynamic value) {
          if (!modifiedFields.contains(key)) {
            record.add(key, value, author: author);
          }
        });
      }
    }
    else if (change["action"] == "remove") {
      collection.removeWhere((d) => d["_id"] == change["_id"], author: author);
    }
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
  Map<String, Map<String, num>> _modifiedFields = {};
  Future get initialSync => _communicator._initialSync.future;
  bool get diffInProgress => _communicator.diffInProgress;
  List<StreamSubscription> _subscriptions = [];

  Subscription.config(this.collectionName, this.collection, this._connection,
      this._communicator, this._author, this._idGenerator, [this.args]);

  Subscription(this.collectionName, this._connection, this._author,
      this._idGenerator, [this.args]) {
    collection = new DataCollection();
    _communicator = new Communicator(_connection, collectionName,
        (List<Map> data) {handleData(data, collection, _author);},
        (List<Map> diff) {handleDiff(diff, this, _author);});
    start();
  }

  void _initDataField(Data data, String field) {
    if (!_modifiedFields.containsKey(data["_id"])) {
      _modifiedFields[data["_id"]] = {};
    }
    
    if (!_modifiedFields[data["_id"]].containsKey(field)) {
      _modifiedFields[data["_id"]][field] = 0;
    }
  }
  
  num tokenForDataField(Data data, String field) {
    _initDataField(data, field);
    return _modifiedFields[data["_id"]][field];
  }
  
  num nextTokenForDataField(Data data, String field) {
    _initDataField(data, field);
    _modifiedFields[data["_id"]][field] += 1;
    return _modifiedFields[data["_id"]][field];
  }
  
  List<String> modifiedDataFields(Data data) {
    if (_modifiedFields.containsKey(data["_id"])) {
      return _modifiedFields[data["_id"]].keys.toList();
    }
    else {
      return [];
    }
  }
  
  void _clearTokenForDataField(Data data, String field) {
    if (_modifiedFields.containsKey(data["_id"])) {
      _modifiedFields[data["_id"]].remove(field);
      
      if (_modifiedFields[data["_id"]].isEmpty) {
        _modifiedFields.remove(data["_id"]);
      }
    }
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
