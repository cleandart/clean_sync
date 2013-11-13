// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of server;

abstract class DataProvider {
  /** Returns collection of items in the following form:
   * {'data': [List<Map>] data, 'version': [num] version_num}
   */
  Future<Map> data();
  /** Returns collection of items in the following form:
   *  {'diff': [List<Map>]} or
   *  {'diff': null, 'data': [List<Map>] data, 'version': [num] version_num}
   */
  Future<Map> diffFromVersion(num version);
  Future add(num id, Map data, String author);
  Future change(num id, Map data, String author);
  Future remove(num id, String author);
}
