// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library hop_runner;

import 'package:hop/hop.dart';
import 'package:hop/hop_tasks.dart';
import '../test/test_all.dart' as test_all;

void main (List<String> args) {
  var paths = ['lib/client.dart', 'lib/server.dart'];

  addTask('docs', createDartDocTask(paths, linkApi: true));
  addTask('analyze', createAnalyzerTask(paths));
  addTask('test', createUnitTestTask(test_all.run));

  runHop(args);
}