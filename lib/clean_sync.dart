// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

/**
 * A library for data subscription and synchronization in single page applications.
 */

library clean_sync;

import 'dart:async';
import "package:clean_server/clean_server.dart";
import "package:clean_data/clean_data.dart";

part 'src/subscription.dart';
part 'src/subscriber.dart';