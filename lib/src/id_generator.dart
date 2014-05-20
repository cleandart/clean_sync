// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

class IdGenerator {
  int _counter = 0;
  String prefix;

  /**
   * Creates IdGenerator with [prefix]
   */
  IdGenerator([this.prefix = ""]);

  String next() {
    _counter++;
    return prefix + '-' + _counter.toRadixString(36);

  }
}
