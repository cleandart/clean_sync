// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.server;

typedef DataProvider DataGenerator(Map args);
Map<String, DataGenerator> _publishedCollections = {};
final int MAX = pow(2,16) - 1;
final int prefix_random_part = new Random().nextInt(MAX);

class Publisher {
  int counter;

  Publisher() {
    counter = 0;
  }

  void publish(String collection, DataGenerator callback) {
    _publishedCollections[collection] = callback;
  }

  bool isPublished(String collection) {
    return _publishedCollections.keys.contains(collection);
  }

  Future handleSyncRequest(ClientRequest request) {
    Map data = request.args;
    print("REQUEST:  ${data}");

    if (data["action"] == "get_id_prefix") {
      return new Future(getIdPrefix).then((prefix) => {'id_prefix': prefix});
    }

    if(!data.containsKey('args')) {
      data['args'] = {};
    }
    data['args']['_authenticated_user_id'] = request.authenticatedUserId;
    DataProvider dp = _publishedCollections[data['collection']](data['args']);

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
      return dp.change(data['data'], data['author']);
    }
    else if (data["action"] == "remove") {
      return dp.remove(data['_id'], data['author']);
    }
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
void publish(String c, DataGenerator dg) {
  PUBLISHER.publish(c, dg);
}

bool isPublished(String collection) {
  return PUBLISHER.isPublished(collection);
}

Future handleSyncRequest(request) {
  return PUBLISHER.handleSyncRequest(request);
}
