// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

final _defaultSubscriptionFactory =
    (collectionName, connection, author, idGenerator, args) =>
        new Subscription(collectionName, connection, author, idGenerator, args);

/**
 * A control object responsible for managing subscription to server published
 * collections.
 *
 * ## Example
 *
 * Create simple Subscriber and subscribe to collection of users.
 *
 *      var subscriber = new Subscriber(connection);
 *      subscriber.init().then((value) {
 *        var usersSubscription = subscriber.subscribe("users", {"olderThan": 18});
 *        useUsers(usersSubscription.collection);
 *      });
 */
class Subscriber {
  Connection _connection;
  String _idPrefix = null;
  final IdGenerator _subscriptionIdGenerator, _dataIdGenerator;
  final _createSubscription;

  /**
   * Dependency injection constructor used mainly in tests.
   */
  Subscriber.config(this._connection, this._dataIdGenerator,
           this._subscriptionIdGenerator, this._createSubscription);


  /**
   * Creates new instance communicating with server using the [connection].
   */
  Subscriber(connection) : this.config(connection, new IdGenerator(),
      new IdGenerator(), _defaultSubscriptionFactory);

  Future _loadIdPrefix() =>_connection.send(
        () => new ClientRequest("sync", {"action" : "get_id_prefix"})
    ).then((response) => response['id_prefix']);

  /**
   * Initialize the subscriber. Using [Subscriber] without proper initialization
   * results in [StateError].
   *
   * The method's purpose is to set an unique [idPrefix] that can be used in
   * '_id' generation in the client code. If the [init] is called without the
   * [idPrefix], it is requested from the server.
   *
   * This method returns the [Future] that completes when the [idPrefix] is
   * obtained and set.
   */
  Future init([idPrefix = null]) {
    idPrefix = (idPrefix == null) ?
        _loadIdPrefix() :
        new Future.value(idPrefix);

    return idPrefix.then((prefix) {
      _idPrefix = prefix;
      _subscriptionIdGenerator.prefix = prefix;
      _dataIdGenerator.prefix = prefix;
    });
  }

  /**
   * Subscribe to [collectionName] published on server. Subscriber must be
   * properly initialized with [init] method before calling [subscribe].
   *
   * Create new [Subscription] to published collection using its
   * [collectionName] and optionally [args]. Data in subscribed collection is
   * keeped in sync between the server and the client with no interaction
   * needed from the user.
   *
   * It is also possible to create multiple independend subscriptions with same
   * [collectionName] and/or [args]. This can be useful when subscribing to
   * really big collection and want to request only a portion of data specified
   * by [args].
   */
  Subscription subscribe(String collectionName, [Map args]) {
    if(_idPrefix == null) {
      throw new StateError("Subscriber can not be used before the Future"
          " returned by 'init' method has completed.");
    }
    String author = _subscriptionIdGenerator.next();
    var subscription = _createSubscription(collectionName, _connection, author,
      _dataIdGenerator, args);
    return subscription;
  }
}
