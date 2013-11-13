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

part 'src/publisher.dart';
part 'src/data_provider.dart';
part 'src/mongo_provider.dart';
