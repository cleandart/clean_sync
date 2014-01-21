// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

part of clean_sync.client;

class MongoComparator {

  static int compareWithKeySelector(dynamic a, dynamic b, Map keySelector){
    int result = 0;

    keySelector.forEach((key, asc_desc){
      int c = compare(a[key],b[key]);

      //if descending, then revert results
      if(c != 0) {
        result = c * asc_desc;
        return result;
      }
    });

    return (result == 0) ? compare(a, b) : result;
  }

  /**
   * Compare one comparable to another.
   *
   * a negative integer if a is smaller than b,
   * zero if a is equal to b, and
   * a positive integer if a is greater than b.
   */

  static int compare(dynamic a, dynamic b) {
    int pa = _priority(a);
    int pb = _priority(b);
    if(pa != pb) return Comparable.compare(pa, pb);

    // Special case : list
    if(a is List){
      List la = a;

      //then b is also []
      if(la.isEmpty) return 0;

      a = la[0];
    }

    if(b is List){
      List lb = b;
      b = lb[0];
    }

    // both should have the same type
    return (a is num || a is String || a is bool || a is DateTime) ? a.compareTo(b) : 0;
  }

  /** http://docs.mongodb.org/manual/reference/bson-types/
    1. MinKey (internal type)
    2. Null
    3. Numbers (ints, longs, doubles)
    4. Symbol, String
    5. Object
    6. Array
    7. BinData
    8. ObjectID
    9. Boolean
    10. Date, Timestamp
    11. Regular Expression
    12. MaxKey (internal type)
  */
  static int _priority(dynamic a, {firstCall : true}){
    // Special case : list
    if(firstCall && a is List){
      List la = a;

      // nothing can beat an empty list
      return (la.isEmpty) ? -1 : _priority(la[0], firstCall : false);
    }

    if(a == null) return 2;
    if(a is num) return 3;
    if(a is String) return 4;
    if(a is Map) return 5;
    if(a is List) return 6;
    if(a is bool) return 9;
    if(a is DateTime) return 10;
    if(a is RegExp || a is Pattern) return 11;

    return 13;
  }
}