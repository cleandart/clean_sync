// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.server;

typedef Future<DataProvider> DataGenerator(Map args);
final int MAX = pow(2,16) - 1;
final int prefix_random_part = new Random().nextInt(MAX);

final Logger _logger = new Logger('clean_sync');

class InvalidArgsException implements Exception {

  dynamic args;

  InvalidArgsException(this.args);

  String toString() => "Args are invalid: $args";

}

/// Representation of resource of data, specified by [generator].
class Resource {
  DataGenerator generator;

  /// Delegates the work to newly created [DataProvider], and returns the
  /// result.
  ///
  /// First, using args extracted from data (data["args"]) are provided to
  /// [generator] to construct [DataProvider]. [DataProvider.data] or
  /// [DataProvider.diffFromVersion] is then called, depending on the value of
  /// data["args"] ("get_data" or "get_diff"). In the latter case
  /// data["version"] is provided as argument to the method.
  Future handleSyncRequest (Map data) {

    var action = data["action"];
    var reqVersion = data['version'];
    DataProvider dp;
    return new Future.value(null)
      .then((_) => generator(data['args']))
      .then((DataProvider _dp) {
        dp = _dp;
        if (action == "get_data") {
          return dp.data().then((result) {
            return result;
          });
        }
        else if(action == "get_diff") {
          return dp.diffFromVersion(reqVersion);
        } else {
          throw new Exception('Publisher: action ${action} not known');
        }
      });

  }

  Resource(this.generator);
}

/// Storage of all published resources. The general idea behind publishing
/// resources is as follows. One may publish a resource under some name on
/// server side, associating it with some [DataGenerator], which is a way of
/// creating [DataProvider] from some parameters. One may also subscribe to this
/// resource on client side (using [Subscriber] and [Subscription]). This starts
/// an automatic machinery that keeps the client-side collection of data in sync
/// with the collection provided by the published resource.
///
/// Example:
/// Let us assume we have already access to [MongoProvider] users, which is
/// associated with some mongo collection of users.
///
/// 1. We may publish resource "users" like this:
///     publisher.publish("users", (_) => users)
/// Subscribing to such resource would then result in having a synced collection
/// of all users on client.
///
/// 2. We may publish resource "users-of-age" like this:
///     publisher.publish("users-of-age", (args) => users.find({"age": args["age"]}))
/// Subscribing to this resource, providing arguments
/// containing "age", would result in having a synced collection of all users
/// with the given age on client. For example,
/// subscription.restart(args: {"age": 20})) would result in having a collection
/// of all users of age 20.
class Publisher {

  Map<String, Resource> _resources = {};

  /// Publishes a [Resource] under the name [collection].
  void publish(String collection, DataGenerator generator) {
    _resources[collection] = new Resource(generator);
  }

  /// Returns [true] if there is a [Resource] published under the name
  /// [collection].
  bool isPublished(String collection) {
    return _resources.containsKey(collection);
  }

  /// Handles [request] generated by [Subscription]. In general, this function
  /// is not expected to be used manually; it is used automatically by the
  /// clean_sync machinery to handle automatic requests from client-side part of
  /// clean_sync library.
  ///
  /// If the form of args provided is wrong, [InvalidArgsException] is thrown.
  ///
  /// [request.args] is expected to be a [Map] of the following form.
  /// Key "collection" should be mapped to the resource name to work with.
  /// Key "args" is expected to be either [null] or a [Map] and it will be
  /// provided to [DataGenerator] associated with the provided resource name;
  /// with "_authenticatedUserId" attached to it.
  /// Key "action" specifies an action to be taken: "get_id_prefix", "get_data"
  /// or "get_diff". The first one returns a unique id prefix, the second one
  /// returns the whole content of the resource, and the third one expects that
  /// also key "version" is provided and returns all differences that happened
  /// to the resource since "version".
  Future handleSyncRequest(ServerRequest request) {
    Map data = request.args;
    _logger.finest("REQUEST:  ${data}");

    if(data['args'] == null) {
      data['args'] = {};
    }
    data['args']['_authenticatedUserId'] = request.authenticatedUserId;

    Resource resource = _resources[data['collection']];
    var action = data["action"];

    if (action == "get_id_prefix") {
      return new Future.value({'id_prefix': getIdPrefix()});
    }

    return resource.handleSyncRequest(data).
    catchError((e, s) {
      if(!e.toString().contains("__TEST__")) {
        if (e is InvalidArgsException) {
          _logger.warning("handle sync request warning: ",e,s);
        } else {
          _logger.shout('handle sync request error: ', e, s);
        }
      }
      return new Future.value({
        'error': e.toString(),
      });
    });

  }
}

/// Instance of [Publisher].
final PUBLISHER = new Publisher();

/// Delegates the work to [PUBLISHER.publish].
void publish(String c, DataGenerator dg) {
  PUBLISHER.publish(c, dg);
}

/// Delegates the work to [PUBLIESHER.isPublished].
bool isPublished(String collection) {
  return PUBLISHER.isPublished(collection);
}

/// Delegates the work to [PUBLIESHER.handleSyncRequest].
Future handleSyncRequest(request) {
  return PUBLISHER.handleSyncRequest(request);
}
