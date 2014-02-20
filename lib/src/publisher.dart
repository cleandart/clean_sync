// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.server;

typedef Future<DataProvider> DataGenerator(Map args);
final int MAX = pow(2,16) - 1;
final int prefix_random_part = new Random().nextInt(MAX);

final Logger logger = new Logger('clean_sync');

class Version {
  Version();
  num value = 0;
}

class Resource {
  DataGenerator generator;
  Function beforeRequestCallback;

  Future handleSyncRequest (Map data) {
    num watchID = startWatch('${data["action"]}, ${data['collection']}');

    var action = data["action"];
    var reqVersion = data['version'];
    List<String> modifications = ['add', 'change', 'remove'];

    Future beforeRequest = new Future.value(null);
    if (beforeRequestCallback != null && modifications.contains(action)) {
      var value;
      if (action == 'add') value = data['data'];
      else if (action == 'change') value = data['change'];
      else if (action == 'remove') value = {};
      beforeRequest = beforeRequestCallback(value, data['args']);
    }

    DataProvider dp;

    return beforeRequest
      .then((_) => generator(data['args']))
      .then((DataProvider _dp) {
        dp = _dp;
        if (action == "get_data") {
          return dp.data().then((result) {
            stopWatch(watchID);
            return result;
          });
        }
        else if(action == "get_diff") {
          return dp.diffFromVersion(reqVersion)
            .then((result) {
                stopWatch(watchID);
                return result;
             });
        }
        else if (action == "add") {
          return dp.add(data['data'], data['author'])
          .then((result) {
            stopWatch(watchID);
            return result;
          });
        }
        else if (action == "change") {
          return dp.change(data['_id'], data['change'], data['author'])
              .then((result) {
                stopWatch(watchID);
                return result;
              });
        }
        else if (action == "remove") {
          return dp.remove(data['_id'], data['author'])
              .then((result) {
                stopWatch(watchID);
                return result;
              });
        }
      });

  }

  Resource(this.generator, this.beforeRequestCallback);
}

class Publisher {
  int counter = 0;

  Map<String, Resource> _resources = {};

  void publish(String collection, DataGenerator generator, {beforeRequest: null}) {
    _resources[collection] = new Resource(generator, beforeRequest);
  }

  bool isPublished(String collection) {
    return _resources.containsKey(collection);
  }

  Future handleSyncRequest(ServerRequest request) {
    Map data = request.args;
    logger.finest("REQUEST:  ${data}");

    if(data['args'] == null) {
      data['args'] = {};
    }
    data['args']['_authenticatedUserId'] = request.authenticatedUserId;

    Resource resource = _resources[data['collection']];
    var action = data["action"];

    if (action == "get_id_prefix") {
      return new Future(getIdPrefix).then((prefix) => {'id_prefix': prefix});
    }

    return resource.handleSyncRequest(data).
    catchError((e, s) {
      if(!e.toString().contains("__TEST__")) {
        logger.shout('handle sync request error:', e, s);
      }
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
