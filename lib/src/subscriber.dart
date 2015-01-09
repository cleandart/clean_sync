// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

final defaultSubscriptionFactory =
    (resourceName, mongoCollectionName, connection, idGenerator, transactor, updateLock) =>
        new Subscription(resourceName, mongoCollectionName, connection,
            idGenerator, transactor, updateLock);

final defaultTransactorFactory =
    (connection, updateLock, author, idGenerator) =>
        new TransactorClient(connection, updateLock, author, idGenerator);
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
  final IdGenerator _dataIdGenerator;
  final _createSubscription;
  final _createTransactor;
  DataReference updateLock;
  final List<Subscription> subscriptions = [];

  /**
   * Dependency injection constructor used mainly in tests.
   */
  Subscriber.config(this._connection, this._dataIdGenerator, this._createSubscription,
           this._createTransactor, this.updateLock);


  /**
   * Creates new instance communicating with server using the [connection].
   */
  Subscriber(connection) : this.config(connection, new IdGenerator(),
      defaultSubscriptionFactory, defaultTransactorFactory, new DataReference(false));

  Future _loadIdPrefix() =>_connection.send(
        () => new ClientRequest("sync", {"action" : "get_id_prefix"})
    ).then((response) => response['id_prefix']);

  /**
   * Initialize the subscriber. Using [Subscriber] without proper initialization
   * results in [StateError].
   *
   * The method's purpose is to set an unique [idPrefix] that can be used in
   * '_id' generation in the client code. If the [init] is called without the
   * [idPrefix], it is requested from the server. If [idPrefix] is provided,
   * everything is set synchronously and there's no need to wait for the Future returned.
   *
   * This method returns the [Future] that completes when the [idPrefix] is
   * obtained and set.
   */
  Future init([idPrefix = null]) {
    if (idPrefix == null) idPrefix = _loadIdPrefix();
    if (idPrefix is Future) {
      return idPrefix.then((prefix) {
        _idPrefix = prefix;
        _dataIdGenerator.prefix = prefix;
      });
    } else {
      _idPrefix = idPrefix;
      _dataIdGenerator.prefix = idPrefix;
      return new Future.value(idPrefix);
    }
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
  Subscription subscribe(String resourceName, String mongoCollectionName) {
    if(_idPrefix == null) {
      throw new StateError("Subscriber can not be used before the Future"
          " returned by 'init' method has completed.");
    }
    var subscription = _createSubscription(resourceName, mongoCollectionName,
        _connection, _dataIdGenerator, __createTransactor(), updateLock);
    subscriptions.add(subscription);
    return subscription;
  }

  TransactorClient __createTransactor(){
    String author = _dataIdGenerator.next();
    return _createTransactor(this._connection, this.updateLock, author, _dataIdGenerator);
  }

  _pruneSubscriptions() {
    subscriptions.removeWhere((sub) => sub.disposed);
  }

  /// Returns string representation of this [Subscriber].
  String toString({data: false}) {
    _pruneSubscriptions();
    String res = "";
    subscriptions.forEach((sub) {
      res += sub.toString()+"\n";
      res += "Resource name: ${sub.resourceName} \n";
      res += "Collection name: ${sub.mongoCollectionName} \n";
      res += "Args: ${sub.args} \n";
      res += "Initial sync completed? ${sub.initialSyncCompleted}\n";
      if (data) {
        res += "Data: ${sub.collection} \n";
      }

    });
    return res;
  }

  /// Returns json representation of this [Subscriber].
  toJson({data: false}) {
    Map res = {};
    subscriptions.forEach((sub) {
      res[sub.resourceName] = {
        'name': sub.resourceName,
        'collectionName': sub.mongoCollectionName,
        'args': sub.args,
        'initialSyncCompleted': sub.initialSyncCompleted,
      };
      if (data) {
        res[sub.resourceName]['data'] = sub.collection;
      }
    });
    return res;
  }
}
