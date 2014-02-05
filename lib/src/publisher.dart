// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.server;

typedef Future<DataProvider> DataGenerator(Map args);
final int MAX = pow(2,16) - 1;
final int prefix_random_part = new Random().nextInt(MAX);

final Logger logger = new Logger('clean_sync');

class Publisher {
  int counter;
  Map<String, DataGenerator> _publishedCollections;
  Map<String, dynamic> _beforeRequestCallbacks;
  Map<String, dynamic> _projections;

  Publisher() {
    _publishedCollections = {};
    _beforeRequestCallbacks = {};
    _projections = {};
    counter = 0;
  }

  void publish(String collection, DataGenerator callback, {beforeRequest: null,
    projection: null}) {
    _publishedCollections[collection] = callback;
    _beforeRequestCallbacks[collection] = beforeRequest;
    _projections[collection] = projection;
  }

  bool isPublished(String collection) {
    return _publishedCollections.keys.contains(collection);
  }

  Future handleSyncRequest(ServerRequest request) {
    Map data = request.args;
    logger.finest("REQUEST:  ${data}");

    if(data['args'] == null) {
      data['args'] = {};
    }
    data['args']['_authenticatedUserId'] = request.authenticatedUserId;

    var dataGenerator = _publishedCollections[data['collection']];
    var beforeRequestCall = _beforeRequestCallbacks[data['collection']];
    var projectionCall = _projections[data['collection']];
    var action = data["action"];

    if (action == "get_id_prefix") {
      return new Future(getIdPrefix).then((prefix) => {'id_prefix': prefix});
    }

    List<String> modifications = ['add', 'change', 'remove'];

    if (modifications.contains(action) && projectionCall != null) {
      throw new Exception('Thou shall not modify projected data!');
    }

    Future beforeRequest = new Future.value(null);
    if (beforeRequestCall != null && modifications.contains(action)) {
      var value;
      if (action == 'add') value = data['data'];
      else if (action == 'change') value = data['change'];
      else if (action == 'remove') value = {};
      beforeRequest = beforeRequestCall(value, data['args']);
    }

    return beforeRequest
        .then((_) => dataGenerator(data['args']))
        .then((DataProvider dp) {
      if (action == "get_data") {
        return dp.data(projection: projectionCall);
      }
      else if (action == "get_diff") {
        return dp.diffFromVersion(data["version"], projection: projectionCall);
      }
      else if (action == "add") {
        return dp.add(data['data'], data['author']);
      }
      else if (action == "change") {
        return dp.change(data['_id'], data['change'], data['author']);
      }
      else if (action == "remove") {
        return dp.remove(data['_id'], data['author']);
      }
    }).catchError((e) {
      return new Future.value({
        'error': e.toString(),
      });
    });
  }

  String getIdPrefix() {
    String prefix =
        new DateTime.now().millisecondsSinceEpoch.toRadixString(36) +
        prefix_random_part.toRadixString(36) + counter.toRadixString(36);
    counter = (counter + 1) % MAX;
    return prefix;
  }
}

final PUBLISHER = new Publisher();
void publish(String c, DataGenerator dg, {beforeRequest: null,
  projection: null}) {
  PUBLISHER.publish(c, dg, beforeRequest: beforeRequest,
      projection: projection);
}

bool isPublished(String collection) {
  return PUBLISHER.isPublished(collection);
}

Future handleSyncRequest(request) {
  return PUBLISHER.handleSyncRequest(request);
}

String getIdPrefix() {
  return PUBLISHER.getIdPrefix();
}
