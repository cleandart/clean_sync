// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of client;

class Subscription {

  Server _server;
  Timer _timer;
  String collection;
  DataCollection data;
  Map args = {};
  num _version = -1;
  String _author;

  Subscription(String collection, Server server, String author, [Map args]) {
    this.collection = collection;
    this._server = server;
    this._author = author;
    this.data = new DataCollection();

    if (args != null) {
      this.args = args;
    }

    _setupListeners();
    _requestInitialData().then((_) => _setupDiffPolling());
  }

  void _setupListeners() {
    data.onChangeSync.listen((event) {
      if (event["author"] == null) {
        event["change"].addedItems.forEach((data) {
          _server.sendRequest(() => new Request("", {
            "action" : "add",
            "collection" : collection,
            "data" : data,
            "author" : _author
          }));
        });

        event["change"].changedItems.forEach((Data data, ChangeSet changeSet) {
          Map change = {};
          changeSet.changedItems.
            forEach((k, Change v) => change[k] = v.newValue);

          _server.sendRequest(() => new Request("", {
            "action" : "change",
            "collection" : collection,
            "_id" : data["_id"],
            "data" : change,
            "author" : _author
          }));
        });

        event["change"].removedItems.forEach((data) {
          _server.sendRequest(() => new Request("", {
            "action" : "remove",
            "collection" : collection,
            "_id" : data["_id"],
            "author" : _author
          }));
        });
      }
    });
  }

  Future _requestInitialData() {
    return _server.sendRequest(() => new Request("", {
      "action" : "get_data",
      "collection" : collection
    })).then((response) {
      _handleResponse(response);
      print("Got initial data, synced to version ${_version}");
      return _version;
    });
  }

  void _setupDiffPolling() {
    _timer = new Timer.periodic(new Duration(seconds: 2), (_) {
      _server.sendRequest(() => new Request("", {
        "action" : "get_diff",
        "collection" : collection,
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
      for (var d in data) {
        toDelete.add(d);
      }
      for (var d in toDelete) {
        data.remove(d, author: _author);
      }
      for (Map record in response['data']) {
        this.data.add(new Data.fromMap(record), author : _author);
      }
      _version = response['version'];
    } else if(response.containsKey('diff') && response['diff'] != null) {
      response['diff'].forEach((change) {
        _applyChange(change);
      });
    }
  }

  void _applyChange(Map change) {
    if (change["author"] != _author || change["collection"] != collection) {
      if (change["action"] == "add") {
        data.add(new Data.fromMap(change["data"]), author: _author);
      }
      else if (change["action"] == "change") {
        Data record = data.firstWhere((d) => d["_id"] == change["_id"]);

        if (record != null) {
          record.addAll( change["data"], author: _author);
        }
      }
      else if (change["action"] == "remove") {
        Data record = data.firstWhere((d) => d["_id"] == change["_id"]);

        if (record != null) {
          data.remove(record, author: _author);
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