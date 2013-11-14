// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

class Subscriber {

//  Map<String, Subscription> _subscribedCollections = {};
  Server _server;
  String _idPrefix;
  int _nextSubscriptionId;

  Subscriber(this._server) {
    _nextSubscriptionId = 0;
  }

  Future init() {
    return _server.sendRequest(
        () => new ClientRequest("", {"action" : "get_id_prefix"}))
      .then((response) {
        _idPrefix = response['id_prefix'];
        print("Got ID prefix: ${_idPrefix}");
        return true;
      });
  }

  Subscription subscribe(String collection, [Map args]) {
    int subscriptionId = _nextSubscriptionId;
    _nextSubscriptionId++;
    String author = _idPrefix + '-' + subscriptionId.toRadixString(36);
    Subscription subscription = new Subscription(collection, _server, author,
        args);
    //_subscribedCollections[collection] = subscription;
    return subscription;
  }
}