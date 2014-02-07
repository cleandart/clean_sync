library benchmark_diff;

import 'dart:async';
import 'dart:core';
import 'package:clean_sync/server.dart' as sync;
import 'package:clean_sync/server.dart';
import 'package:clean_ajax/server.dart';
import 'package:logging/logging.dart';

const ELEMENTS = 1000;
const CLIENTS = 1000;
final TIME = new Duration(seconds: 5);

var mongodb;
setup() {
  mongodb = new MongoDatabase('mongodb://0.0.0.0/benchmark_diff');
  return Future.wait(mongodb.init)
  .then((_) => mongodb.dropCollection('benchmark'))
  .then((_) => mongodb.removeLocks())
  .then((_){
    return new Future.value(mongodb.collection('benchmark'));
  });
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
    publish('benchmark', (_) => mongodb.collection('benchmark'));

    var request = new ServerRequest("sync", {
      "action" : "get_diff", "collection" : 'benchmark',
       "version" : version }, null, null);

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
      sync.handleSyncRequest(request).then((_) { countRequest++; createRequest(); });
    }
    for(int i=0; i < CLIENTS; i++) {
      createRequest();
    }
  });
}
