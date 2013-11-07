// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync_client;

class Subscriber {

  Map<String, Subscription> _subscribedCollections = {};
  Server _server;
  String _author;

  Subscriber(this._server, this._author);

  Subscription subscribe(String collection, [Map args]) {
    DataCollection data = new DataCollection();
    Subscription subscription = new Subscription(collection, _server, data, _author, args);

    _subscribedCollections[collection] = subscription;

    return subscription;
  }
}