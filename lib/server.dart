// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

/**
 * A library for data subscription and synchronization in single page
 * applications.
 */

library clean_sync.server;

import 'dart:async';
import 'package:mongo_dart/mongo_dart.dart';
import 'package:clean_ajax/server.dart';
import 'dart:math';
import "package:logging/logging.dart";

part 'src/publisher.dart';
part 'src/data_provider.dart';
part 'src/mongo_provider.dart';

// profiling
Map watches = {};
var watchID = 0;

num startWatch(action, colName) {
  watchID++;
  watches[watchID] = [new Stopwatch()..start(), action, colName];
  logger.info('$watchID Started processing request ($action, $colName).');
  return watchID;
}
stopWatch(watchID) {
  var watch = watches[watchID][0];
  var action = watches[watchID][1];
  var colName = watches[watchID][2];
  logger.info('$watchID Processing request ($action, $colName) took ${watch.elapsed}.');
  watch.stop();
  watches.remove(watchID);
}

getElapsed(watchID) {
  var watch = watches[watchID][0];
  var action = watches[watchID][1];
  var colName = watches[watchID][2];
  logger.info('$watchID Processing request ($action, $colName) currently elapsed ${watch.elapsed}.');
}
// end profiling
