// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

class Communicator {
  Connection _connection;
  String _collectionName;
  Function _handleData, _handleDiff;
  bool _stopped = true;
  num _version;

  Communicator(this._connection, this._collectionName, this._handleData,
      this._handleDiff);

  void start() {
    _stopped = false;
    // request initial data
    _connection.sendRequest(() => new ClientRequest("sync", {
      "action" : "get_data",
      "collection" : _collectionName
    })).then((response) {
      _version = response['version'];
      _handleData(response['data']);
      print("Got initial data, synced to version ${_version}");
      if(!_stopped) {
        _requestDiff();
      }
    });
  }

  void stop() {
    _stopped = true;
  }

  void resume() {
    _requestDiff();
  }

  void _requestDiff() {
    _connection.sendRequest(() => new ClientRequest("sync", {
        "action" : "get_diff",
        "collection" : _collectionName,
        "version" : _version
      })).then((response) {
        // id data and version was sent, diff is set to null
        if(response['diff'] == null) {
          _version = response['version'];
          _handleData(response['data']);
        } else {
          if(!response['diff'].isEmpty) {
            _version = response['diff'].map((item) => item['version'])
                .reduce(max);
            _handleDiff(response['diff']);
          }
        }
        if(!_stopped){
          _requestDiff();
        }
      });
  }
}