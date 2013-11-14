// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of client;

class Subscriber {

  Map<String, Subscription> _subscribedCollections = {};
  Server _server;
  String _author;

  Subscriber(this._server, this._author);

  Subscription subscribe(String collection, [Map args]) {

    Subscription subscription = new Subscription(collection, _server,
      _author, args);

    _subscribedCollections[collection] = subscription;

    return subscription;
  }
}