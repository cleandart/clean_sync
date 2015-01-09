// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.server;

/// Interface for working with some collection of data. Provides methods for
/// querying current state of the data ([data] and [diffFromVersion]) and also
/// methods for modification ([add], [remove], [change], [changeJson]).
abstract class DataProvider {
  /** Returns collection of items in the following form:
   * {'data': [List<Map>] data, 'version': [num] version_num}
   */
  Future<Map> data();

  /** Returns collection of items in the following form:
   *  {'diff': [List<Map>]} or
   *  {'diff': null, 'data': [List<Map>] data, 'version': [num] version_num}
   *
   *  If 'diff' value is not null, items in the list are of following form:
   *  {'action': 'add'/'change',
   *   '_id': 'value0',
   *   'author': 'Some String',
   *   'data': {'_id': 'value0', 'field1': 'value1', 'field2': 'value2', ...}
   *   'version': 5}
   *
   *  or
   *
   *  {'action': 'remove',
   *   '_id': 'value0',
   *   'author': 'Some String',
   *   'version': 5}
   *
   *  In case of 'add', value of 'data' is a [Map] representing new data that
   *  was added. In case of 'change', value of 'data' is a [Map] containing new
   *  key-value pairs and/or pairs of already existing keys and updated values.
   */
  Future<Map> diffFromVersion(num version);

  /// Changes record/document with [_id] according to [jsonData].
  ///
  /// [jsonData] can be a 2-element [List] or [Map]. In the first case the
  /// elements are "from" and "to" versions of the data; using
  /// [CLEAN_UNDEFINED] in the list expresses adds or removes. In the latter
  /// case, the [Map] is just the "to" version of the data (and the action is
  /// "change" by convention).
  /// [author] is recorded as the author of the change (this author is then
  /// provided by [diffFromVersion]). [clientVersion] is used by the client-side
  /// clean_sync library and does not have to be provided when using this method
  /// on server side.
  Future changeJson(String _id, jsonData, String author, {clientVersion: null, upsert: false});

  /// Adds new record/document [data] to the collection.
  ///
  /// [author] is recorded as the author of the change (this author is then
  /// provided by [diffFromVersion]). [clientVersion] is used by the client-side
  /// clean_sync library and does not have to be provided when using this method
  /// on server side.
  Future add(Map data, String author, {String clientVersion : null});

  /// Changes record/document with [id] to new value provided by [change].
  ///
  /// [author] is recorded as the author of the change (this author is then
  /// provided by [diffFromVersion]). [clientVersion] is used by the client-side
  /// clean_sync library and does not have to be provided when using this method
  /// on server side.
  Future change(String id, Map change, String author, {String clientVersion : null});

  /// Removes the record/document with [id].
  ///
  /// [author] is recorded as the author of the change (this author is then
  /// provided by [diffFromVersion]). [clientVersion] is used by the client-side
  /// clean_sync library and does not have to be provided when using this method
  /// on server side.
  Future remove(String id, String author, {String clientVersion : null});
}
