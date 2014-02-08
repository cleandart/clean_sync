// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.server;

typedef Future<DataProvider> DataGenerator(Map args);
final int MAX = pow(2,16) - 1;
final int prefix_random_part = new Random().nextInt(MAX);

final Logger logger = new Logger('clean_sync');

class Version {
  Version(){
    print('CONSTRUCTING VERSION!!');
  }
  num value = 0;
}

class Resource {
  DataGenerator generator;
  Function beforeRequestCallback;
  Function projection;
  Version version;


  Future handleSyncRequest (Map data) {
    var action = data["action"];
    var reqVersion = data['version'];
    List<String> modifications = ['add', 'change', 'remove'];

    if (modifications.contains(action) && projection != null) {
      throw new Exception('Thou shall not modify projected data!');
    }

    Future beforeRequest = new Future.value(null);
    if (beforeRequestCallback != null && modifications.contains(action)) {
      var value;
      if (action == 'add') value = data['data'];
      else if (action == 'change') value = data['change'];
      else if (action == 'remove') value = {};
      beforeRequest = beforeRequestCallback(value, data['args']);
    }

    var myVer = version.value;

    MongoProvider dp;
    num maxVersion;

    return beforeRequest
        .then((_) => generator(data['args']))
        .then((DataProvider _dp) {
          dp = _dp;
          return dp.maxVersion.then((v){
            if(action == 'get_diff') {
//              print('maxversion: $v, reqversion: $reqVersion');
              maxVersion = v;
              if(maxVersion<myVer){
                print('ZAUJIMAVE max: ${maxVersion} my: ${myVer}');
                dp.maxVersion.then((ver){
                  print('este raz: $ver');
                });
              }
//              assert(myVer<=maxVersion);
            }
          });
        }).then((_){
      if (action == "get_data") {
        return dp.data(projection: projection);
      }
      else if (action == "get_diff") {
//        print('req version: $reqVersion myversion: ${version.value}');
        if (reqVersion == myVer) {
          return new Future.delayed(new Duration(milliseconds: 10), () => {'diff': [], 'version': myVer});
        } else {
          return dp.diffFromVersion(reqVersion, projection: projection)
          .then((diff){
            if(diff.isEmpty){
//              assert(myVer<=maxVersion);
              assert(myVer!=null);
              return {'diff': diff, 'version': myVer };
            } else {
              return {'diff': diff};
            }
          });
        }
      }
      else if (action == "add") {
        return memoizeVersion(dp.add(data['data'], data['author']), 'add');
      }
      else if (action == "change") {
        return memoizeVersion(dp.change(data['_id'], data['change'], data['author']), 'change');
      }
      else if (action == "remove") {
        return memoizeVersion(dp.remove(data['_id'], data['author']), 'remove');
      }
    });

  }

  memoizeVersion(Future<num> result, [action]){
    return result.then((val){
      if (val is num) {
        version.value = val;
      }
      print('memoize: $val (${action})');
      return val;
    });
  }

  Resource(this.generator, this.beforeRequestCallback, this.projection, this.version);
}

class Publisher {
  int counter = 0;

  Map<String, Resource> _resources = {};
  Map<String, Version> _versions = {};

  Publisher();

  void publish(String collection, DataGenerator generator, {beforeRequest: null,
    projection: null, collectionName: null}) {
    Version ver;
    if (collectionName != null && _versions.containsKey(collectionName)) {
      ver = _versions[collectionName];
    } else {
      ver = new Version();
      _versions[collectionName] = ver;
    }
    _resources[collection] = new Resource(generator, beforeRequest, projection, ver);
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
      logger.shout('error', e, s);
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
