library benchmark_diff;

import 'dart:async';
import 'dart:core';
import 'package:clean_sync/server.dart';
import 'package:clean_ajax/server.dart';
import 'package:logging/logging.dart';
import 'package:clean_lock/lock_requestor.dart';


// 1k elem, 50clients: 640, 770, 1420
const ELEMENTS = 1000;
const CLIENTS = 50;

final TIME = new Duration(seconds: 10);

MongoConnection mongoConnection;
setup() {
  Cache cache = new Cache(new Duration(milliseconds: 200), 10000);

  return LockRequestor.connect("127.0.0.1", 27002)
  .then((LockRequestor lockRequestor) => mongoConnection = new MongoConnection('mongodb://0.0.0.0/benchmark', lockRequestor, cache: cache))
  .then((_) => mongoConnection.init())
  .then((_) => mongoConnection.transact((MongoDatabase mdb) => mdb.dropCollection('benchmark')))
  .then((_) => mongoConnection.transact((MongoDatabase mdb) => mdb.createCollection('benchmark')))
  .then((_) => mongoConnection.collection('benchmark'));
}

main() {
  MongoProvider collection;
  print('Initiliazing....');
  setup().then((MongoProvider col) {
    collection = col;
    num elements = 0;
    print('Inserting data into Mongo with $ELEMENTS elements....');
    return collection.addAll(new List.generate(ELEMENTS, (id) =>
      { '_id': '$id',
        ".-": "A", "-...": "B", "-.-.": "C", "-..": "D", ".": "E", "..-.": "F",
        "--.": "G", "....": "H", "..": "I", ".---": "J", "-.-": "K", ".-..": "L",
        "--": "M", "-.": "N", "---": "O", ".--.": "P", "--.-": "Q", ".-.": "R",
        "...": "S", "-": "T", "..-": "U", "...-": "V",  ".--": "W", "-..-": "X",
        "-.--": "Y", "--..": "Z"
        }), 'benchmark');
  }).then((_) {
    return collection.data().then((data) => data['version']);
  }).then((version) {
    print('Publishing collection with version $version....');
    var versionProvider = mongoConnection.collection('benchmark');
    publish('benchmark', (_) => mongoConnection.collection('benchmark'));

    var request = new ServerRequest("sync", {
      "action" : "get_diff", "collection" : 'benchmark',
       "version" : version-20}, null, null);

    Stopwatch stopwatch = new Stopwatch();
    num countRequest = 0;
    bool stop = false;

    new Timer(TIME, () {
      stop = true;
      stopwatch.stop();
      print('Duration of test is: ${stopwatch.elapsedMilliseconds} milliseconds.');
      print('It has been ${countRequest} diff calls called.');
      num diffSpeed = countRequest/stopwatch.elapsedMilliseconds*1000;
      print('It has been ${diffSpeed.toStringAsFixed(2)} diff'
      'calls per second.');
    });

    print('Starting benchmark for ${TIME.inSeconds} seconds....');
    stopwatch.start();


    createRequest() {
      if(stop) return;
//      collection.maxVersion.then((_){ countRequest++; createRequest();});
      handleSyncRequest(request).then((_) { countRequest++; createRequest(); });
    }
    for(int i=0; i < CLIENTS; i++) {
      createRequest();
    }
  });
}
