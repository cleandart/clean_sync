// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

final _defaultSubscriptionFactory =
    (collectionName, connection, idGenerator, transactor, updateLock) =>
        new Subscription(collectionName, connection, idGenerator, transactor, updateLock);

final _defaultTransactorFactory =
    (connection, updateLock, author, idGenerator) =>
        new Transactor(connection, updateLock, author, idGenerator);
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
  final _createTransactor;
  DataReference updateLock;

  /**
   * Dependency injection constructor used mainly in tests.
   */
  Subscriber.config(this._connection, this._dataIdGenerator,
           this._subscriptionIdGenerator, this._createSubscription,
           this._createTransactor, this.updateLock);


  /**
   * Creates new instance communicating with server using the [connection].
   */
  Subscriber(connection) : this.config(connection, new IdGenerator(),
      new IdGenerator(), _defaultSubscriptionFactory, _defaultTransactorFactory,
      new DataReference(false));

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
   * [collectionName]. Data in subscribed collection is
   * kept in sync between the server and the client with no interaction
   * needed from the user.
   *
   * In order to start this subscription, restart(args) has to be called on it
   *
   * It is also possible to create multiple independend subscriptions with same
   * [collectionName]. This can be useful when subscribing to
   * really big collection and want to request only a portion of data specified
   * by args in restart method.
   */
  Subscription subscribe(String collectionName) {
    if(_idPrefix == null) {
      throw new StateError("Subscriber can not be used before the Future"
          " returned by 'init' method has completed.");
    }
    var subscription = _createSubscription(collectionName, _connection, _dataIdGenerator,
      createTransactor(), updateLock);
    return subscription;
  }

  Transactor createTransactor(){
    String author = _subscriptionIdGenerator.next();
    return _createTransactor(this._connection, this.updateLock, author, _dataIdGenerator);
  }
}
