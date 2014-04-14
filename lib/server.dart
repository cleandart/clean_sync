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
import 'dart:collection';
import 'dart:convert';
import 'package:useful/useful.dart';
import 'package:useful/useful.dart' as useful;
import "dart:io";
import "package:clean_data/clean_data.dart";
import 'package:clean_sync/mongo_client.dart';

part 'src/publisher.dart';
part 'src/data_provider.dart';
part 'src/mongo_provider.dart';
part 'src/cache.dart';

// profiling
Logger profilingLogger = new Logger('profiling');

Map watches = {};
var watchID = 0;

num startWatch(identifier) {
  watchID++;
  watches[watchID] = [new Stopwatch()..start(), identifier];
  profilingLogger.info('$watchID Started processing request ($identifier).');
  return watchID;
}
stopWatch(watchID) {
  var watch = watches[watchID][0];
  var identifier = watches[watchID][1];
  profilingLogger.info('$watchID Processing request ($identifier) took ${watch.elapsed}.');
  watch.stop();
  watches.remove(watchID);
}

logElapsedTime(watchID) {
  var watch = watches[watchID][0];
  var identifier = watches[watchID][1];
  profilingLogger.info('$watchID Processing request ($identifier) currently elapsed '
              '${watch.elapsed}.');
}
// end profiling
