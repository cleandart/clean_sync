// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

class Subscriber {

  Map<String, Subscription> _subscribedCollections = {};
  Server _server;
  String _author_prefix;

  Subscriber(this._server, this._author_prefix);

  Subscription subscribe(String collection, [Map args]) {

    String author = _author_prefix + collection;
    Subscription subscription = new Subscription(collection, _server,
      author, args);

    _subscribedCollections[collection] = subscription;

    return subscription;
  }
}