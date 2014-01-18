// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

class Communicator {
  Connection _connection;
  String _collectionName;
  Function _handleData, _handleDiff;
  String _updateStyle;
  bool _stopped = true;
  num _version;
  Completer _initialSync = new Completer();
  bool _diffInProgress = false;
  
  bool get diffInProgress => _diffInProgress;

  Communicator(this._connection, this._collectionName, this._handleData,
      this._handleDiff, [this._updateStyle='diff']);

  void start() {
    _stopped = false;
    // request initial data
    _connection.send(() => new ClientRequest("sync", {
      "action" : "get_data",
      "collection" : _collectionName
    })).then((response) {
      _version = response['version'];
      _handleData(response['data']);
      print("Got initial data, synced to version ${_version}");
      if (!_initialSync.isCompleted)
        _initialSync.complete();
      if(!_stopped) {
        if (_updateStyle == 'diff') {
          _requestDiff();
        }
        if (_updateStyle == 'data') {
          _requestData();
        }
      }
    });
  }

  void stop() {
    _stopped = true;
  }

  void resume() {
    _requestDiff();
  }

  void _requestData() {
    _connection.send(() => new ClientRequest("sync", {
      "action" : "get_data",
      "collection" : _collectionName
    })).then((response) {
      _version = response['version'];
      _handleData(response['data']);
      if(!_stopped) {
        _requestData();
      }
    });
  }

  void _requestDiff() {

    _connection.send(() {
      _diffInProgress = true;

      return new ClientRequest("sync", {
        "action" : "get_diff",
        "collection" : _collectionName,
        "version" : _version
      });
    }).then((response) {
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
      
      _diffInProgress = false;
    });
  }
}