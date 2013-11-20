// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

final _defaultSubscriptionFactory =
    (collectionName, connection, author, idGenerator, args) =>
        new Subscription(collectionName, connection, author, idGenerator, args);

class MissingIdPrefixException implements Exception {
   final String msg;
   const MissingIdPrefixException([this.msg]);
   String toString() => msg == null ? 'DiffNotPossible' : msg;
}

class Subscriber {
  Connection _connection;
  String _idPrefix = null;
  final IdGenerator _subscriptionIdGenerator, _dataIdGenerator;
  final _createSubscription;

  Subscriber.config(this._connection, this._dataIdGenerator,
           this._subscriptionIdGenerator, this._createSubscription);

  Subscriber(this._connection)
      : _dataIdGenerator = new IdGenerator(),
        _subscriptionIdGenerator = new IdGenerator(),
        _createSubscription = _defaultSubscriptionFactory;

  Future init() {
    return _connection.sendRequest(
        () => new ClientRequest("sync", {"action" : "get_id_prefix"}))
      .then((response) {
        _idPrefix = response['id_prefix'];
        _subscriptionIdGenerator.prefix = _idPrefix;
        _dataIdGenerator.prefix = _idPrefix;
        return true;
      });
  }

  Subscription subscribe(String collectionName, [Map args]) {
    if(_idPrefix == null) {
      throw new MissingIdPrefixException(
          "init() has to be called and completed first.");
    }
    String author = _subscriptionIdGenerator.next();
    var subscription = _createSubscription(collectionName, _connection, author,
      _dataIdGenerator, args);
    return subscription;
  }
}
