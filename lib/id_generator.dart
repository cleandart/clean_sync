// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library clean_sync.id_generator;

import 'dart:math';

final int MAX = pow(2,16) - 1;
final int prefix_random_part = new Random().nextInt(MAX);
int counter = 0;

class IdGenerator {
int _counter = 0;
String prefix;

  /** Creates IdGenerator with [prefix] */
  IdGenerator([this.prefix = ""]);

  String next() {
   _counter++;
   return prefix + '-' + _counter.toRadixString(36);

  }
}

/** Generates random id prefix that can be provided to IdGenerator. */
String getIdPrefix() {
  String prefix =
      new DateTime.now().millisecondsSinceEpoch.toRadixString(36) +
      prefix_random_part.toRadixString(36) + counter.toRadixString(36);
  counter = (counter + 1) % MAX;
  return prefix;
}