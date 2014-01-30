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

  Publisher() {
    _publishedCollections = {};
    _beforeRequestCallbacks = {};
    counter = 0;
  }

  void publish(String collection, DataGenerator callback, {beforeRequest: null}) {
    _publishedCollections[collection] = callback;
    _beforeRequestCallbacks[collection] = beforeRequest;
  }

  bool isPublished(String collection) {
    return _publishedCollections.keys.contains(collection);
  }

  Future handleSyncRequest(ServerRequest request) {
    Map data = request.args;
    logger.finest("REQUEST:  ${data}");

    if (data["action"] == "get_id_prefix") {
      return new Future(getIdPrefix).then((prefix) => {'id_prefix': prefix});
    }

    if(data['args'] == null) {
      data['args'] = {};
    }
    data['args']['_authenticatedUserId'] = request.authenticatedUserId;

    Future beforeRequest = new Future.value(null);
    if (_beforeRequestCallbacks[data['collection']] != null) {
      if (data['action'] == 'add' || data['action'] == 'change' || data['action'] == 'remove') {
        var value;
        if (data['action'] == 'add') value = data['data'];
        else if (data['action'] == 'change') value = data['change'];
        else if (data['action'] == 'remove') value = {};
        beforeRequest = _beforeRequestCallbacks[data['collection']](value, data['args']);
      }
    }

    return beforeRequest.then((_) => _publishedCollections[data['collection']](data['args'])).then((DataProvider dp) {
      if (data["action"] == "get_data") {
        return dp.data();
      }
      else if (data["action"] == "get_diff") {
        return dp.diffFromVersion(data["version"]);
      }
      else if (data["action"] == "add") {
        return dp.add(data['data'], data['author']);
      }
      else if (data["action"] == "change") {
        return dp.change(data['_id'], data['change'], data['author']);
      }
      else if (data["action"] == "remove") {
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
void publish(String c, DataGenerator dg, {beforeRequest: null}) {
  PUBLISHER.publish(c, dg, beforeRequest: beforeRequest);
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
