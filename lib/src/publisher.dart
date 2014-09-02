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
    DataProvider dp;
    return new Future.value(null)
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
        } else {
          throw new Exception('Publisher: action ${action} not known');
        }
      });

  }

  Resource(this.generator);
}

class Publisher {
  int counter = 0;

  Map<String, Resource> _resources = {};

  void publish(String collection, DataGenerator generator) {
    _resources[collection] = new Resource(generator);
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
      return new Future.value({'id_prefix': getIdPrefix()});
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
