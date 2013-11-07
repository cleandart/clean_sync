// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync_server;

abstract class DataProvider {
  Future<List<Map>> data();
  Future<List<Map>> diffFromVersion(num version);
  Future add(num id, Map data, String author);
  Future change(num id, Map data, String author);
  Future remove(num id, String author);
}