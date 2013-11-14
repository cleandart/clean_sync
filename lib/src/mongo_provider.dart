// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of server;

class DiffNotPossibleException implements Exception {
   final String msg;
   const DiffNotPossibleException([this.msg]);
   String toString() => msg == null ? 'DiffNotPossible' : msg;
}


class MongoProvider implements DataProvider {
  String _mongoUrl;
  String _collectionName;
  Map _selector = {};
  static num _maxVersion;

  num get maxVersion => _maxVersion;

  MongoProvider(this._mongoUrl, [this._collectionName]);

  Future initialize(List collections) {
    return _query((db) {
      return Future.wait(collections.map((String collection) {
        return this.collection(collection)._collectionHistory(db).count();
      }).toList());
    }).then((versions) => _maxVersion = versions.reduce((value, element) => value = (element > value) ? element : value));
  }

  MongoProvider collection(String collection) {
    return new MongoProvider(_mongoUrl, collection);
  }

  DbCollection _collection(Db db) {
    return db.collection(_collectionName);
  }

  DbCollection _collectionHistory(Db db) {
    return db.collection("${_collectionName}_history");
  }

  num _nextVersion() {
    return ++_maxVersion;
  }

  MongoProvider find(Map params) {
    _selector.addAll(params);
    return this;
  }

  Future<List> all() {
    return _query((db) {
      return _collection(db).find(_selector).toList();
    });
  }

  Future _query(query) {
    Db db = new Db(_mongoUrl);
    Future result = db.open().then((_) => db).then(query);
    result.then((_) => db.close());
    return result;
  }

  Future add(num _id, Map data, String author) {
    return _query((db) {
      return _collectionHistory(db).insert({
        "before" : {},
        "after" : data,
        "change" : {},
        "action" : "add",
        "author" : author,
        "version" : _nextVersion()
      }).then((_) {
        return _collection(db).insert(data);
      });
    });
  }

  Future change(num id, Map data, String author) {
    return _query((db) {
      return _collection(db).findOne({"_id" : id}).then((Map record) {
        Map newRecord = new Map.from(record);
        newRecord.addAll(data);

        return _collectionHistory(db).insert({
          "before" : record,
          "after" : newRecord,
          "change" : data,
          "action" : "change",
          "author" : author,
          "version" : _nextVersion()
        }).then((_) {
          return _collection(db).save(newRecord);
        });
      });
    });
  }

  Future remove(num id, String author) {
    return _query((db) {
      return _collection(db).findOne({"_id" : id}).then((Map record) {
        return _collectionHistory(db).insert({
          "before" : record,
          "after" : {},
          "change" : {},
          "action" : "remove",
          "author" : author,
          "version" : _nextVersion()
        }).then((_) {
          return _collection(db).remove({"_id" : record["_id"]});
        });
      });
    });
  }

  Future<Map> data() {
    return all().then((d) => {'data': d, 'version': _maxVersion});
  }

  Future<Map> diffFromVersion(num version) {
    try{
      return _diffFromVersion(version).then((d) => {'diff': d});
    } on DiffNotPossibleException catch(e) {
      return data().then((d) {
        d['diff'] = null;
        return d;
      });
    }
  }

  Future<List<Map>> _diffFromVersion(num version) {
    // if (some case not covered so far) {
    // throw new DiffNotPossibleException('diff not possible');

    Map beforeSelector = {"\$query" : {"version" : {"\$gt" : version}, "before" : {"\$gt" : {}}}, "\$orderby" : {"version" : 1}};
    Map afterSelector = {"\$query" : {"version" : {"\$gt" : version}, "after" : {"\$gt" : {}}}, "\$orderby" : {"version" : 1}};

    _selector.forEach((k, v) {
      beforeSelector["\$query"]["before.${k}"] = v;
      afterSelector["\$query"]["after.${k}"] = v;
    });

    return _query((db) {
      return Future.wait([
        _collectionHistory(db).find(beforeSelector).toList(),
        _collectionHistory(db).find(afterSelector).toList(),
      ]).then((responses) {
        List before = responses[0];
        List after = responses[1];
        List diff = [];

        int i = 0;
        int j = 0;

        while (i < before.length && j < after.length) {
          if (before[i]["version"] < after[j]["version"]) {
            diff.add({
              "action" : "remove",
              "_id" : before[i]["before"]["_id"],
              "version" : before[i]["version"],
              "author" : before[i]["author"],
              "collection" : _collectionName
            });

            i++;
          }
          else if (before[i]["version"] == after[j]["version"]) {
            diff.add({
              "action" : "change",
              "_id" : before[i]["before"]["_id"],
              "data" : before[i]["change"],
              "version" : before[i]["version"],
              "author" : before[i]["author"],
              "collection" : _collectionName
            });

            i++;
            j++;
          }
          else {
            diff.add({
              "action" : "add",
              "data" : after[j]["after"],
              "version" : after[j]["version"],
              "author" : after[j]["author"],
              "collection" : _collectionName
            });

            j++;
          }
        }

        while (i < before.length) {
          diff.add({
            "action" : "remove",
            "_id" : before[i]["before"]["_id"],
            "version" : before[i]["version"],
            "author" : before[i]["author"],
            "collection" : _collectionName
          });

          i++;
        }

        while (j < after.length) {
          diff.add({
            "action" : "add",
            "data" : after[j]["after"],
            "version" : after[j]["version"],
            "author" : after[j]["author"],
            "collection" : _collectionName
          });

          j++;
        }

        return diff;
      });
    });
  }
}
