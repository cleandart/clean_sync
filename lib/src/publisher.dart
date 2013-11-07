// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync_server;

typedef DataProvider DataGenerator(Map args);
Map<String, DataGenerator> _publishedCollections = {};

class Publisher {
  void publish(String collection, DataGenerator callback) {
    _publishedCollections[collection] = callback;
  }

  bool isPublished(String collection) {
    return _publishedCollections.keys.contains(collection);
  }

  Future handleSyncRequest(request) {
    print('lala');
    Map data = request['args'];
    print("REQUEST:  ${data}");

    DataProvider dp = _publishedCollections[data['collection']](data['args']);

    if (data["action"] == "get_data") {
      return dp.data();
    }
    else if (data["action"] == "get_diff") {
      return dp.diffFromVersion(data["version"]);
    }
    else if (data["action"] == "add") {
      return dp.add(data['_id'], data['data'], data['author']);
    }
    else if (data["action"] == "change") {
      return dp.change(data['_id'], data['data'], data['author']);
    }
    else if (data["action"] == "remove") {
      return dp.remove(data['_id'], data['author']);
    }
  // return new Future.value({"action" : data["action"]});
  }
}

var default_publisher = new Publisher();
void publish(String c, DataGenerator dg) {
  default_publisher.publish(c, dg);
}

bool isPublished(String collection) {
  return default_publisher.isPublished(collection);
}

Future handleSyncRequest(request) {
  return default_publisher.handleSyncRequest(request);
}
