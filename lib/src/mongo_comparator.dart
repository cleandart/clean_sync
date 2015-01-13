// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

/**
 * Experimental feature, not yet meant for use in production.
 *
 * Current implementation works as follows. If the two objects are not lists,
 * then are compared as in the documentation: http://docs.mongodb.org/manual/reference/bson-types/
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

  If comparing a object versus list, then the MIN_ELEMENT of list is used,
    e.g. 1 == [1] && 1 < [2] && 1 < [[1]]
    Note in the last case [1] is used for the right side comparison.

  Special case is empty list, whose MIN_ELEMENT is "undefined" and:
    [] < null && [] < [null] && null < [[]]

  If comparing two list objects, the result is given by comparing their MIN_ELEMENTS
    !but now (nested) lists are treated always as lists, so:
    [null] < [[]] && [[]] < [[[]]] && [["whatever"], null] == [null] && [[]] < [false]
*/


//part of clean_sync.client;

class MongoComparator {

  //TODO test wtih several keySelectors !
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

    if(result == 0){
      int a_version = a["__clean_version"];
      int b_version = b["__clean_version"];
      return a_version.compareTo(b_version);
    }

    return result;
  }

  /**
   * Compare one comparable to another.
   *
   * a negative integer if a is smaller than b,
   * zero if a is equal to b, and
   * a positive integer if a is greater than b.
   */

  static List preprocessInput(dynamic orig, {inList : false}) {
    // Special case : list - first "MongoComparable" element is used for comparision
    //   if doesn't exists then the first element.
    // Special special case: empty list
    // Special special special case: [[]] because [{}] < [[]] < [[null]]
    if(inList == false){
      int p_orig = priority(orig, inList : inList);

      if(p_orig == TYPE_EMPTY_LIST) return [p_orig, []];
      //@precondition: p_orig is not an empty list
      return orig is List ? getListMinElement(orig)
            : [priority(orig, inList: false),orig];
    }
    else{
      return [priority(orig, inList: true), orig];
    }
  }

  static int compare(dynamic orig_a, dynamic orig_b, {inList : false}) {
    List obj_a = preprocessInput(orig_a, inList : inList);
    List obj_b = preprocessInput(orig_b, inList : inList);
    int pa = obj_a[0]; // priorities
    int pb = obj_b[0];
    dynamic ca = obj_a[1]; //compareTo
    dynamic cb = obj_b[1];

    if(pa != pb) {
      return pa.compareTo(pb);
    }
    //@precondtion: priority(a) == priority(b)

    // bool.compareTo() doesn't exist
    if(pa == TYPE_BOOL){
       int ia = ca ? 1 : 0;
       int ib = cb ? 1 : 0;
       return ia.compareTo(ib);
    }

    // empty list scenario
    if(isEmptyList(ca) && isEmptyList(cb)){
      return 0;
    }
    if(isEmptyList(ca)){
      return -1;
    }
    if(isEmptyList(cb)){
      return 1;
    }

    if(pa == TYPE_NULL){ // && b == null // given by the @precondition
      return 0;
    }

    //compare lists recursively
    if(ca is List && cb is List){
      //resolve specia special special case: [[]] , because TrueComparer is [[]] but
      return compare(getListMinElement(ca)[1],getListMinElement(cb)[1], inList:true);
    }
    return (isMongoComparable(ca)) ? ca.compareTo(cb) : 0;
  }

  static bool isMongoComparable(dynamic a){
    return (a == null || a is num || a is String || a is bool || a is DateTime);
  }

  static bool isEmptyList(dynamic list){
    if(!(list is List)) return false;
    return priority(list, inList:false) == TYPE_EMPTY_LIST;
  }

  // returns [priority, object to compare with
  // if empty list then exception
  static dynamic getListMinElement(List list){
    int result = 0;
    for(int i=1; i<list.length ; i++){
      if(compare(list[i], list[result], inList:true) < 0) {
        result = i;
      }
    }
    return [priority(list[result], inList:true), list[result]];
  }

  static final int TYPE_EMPTY_LIST = -1;
  static final int TYPE_NULL = 2;
  static final int TYPE_NUM = 3;
  static final int TYPE_STRING = 4;
  static final int TYPE_MAP = 5;
  static final int TYPE_LIST = 6;
  static final int TYPE_BOOL = 7;
  static final int TYPE_DATETIME = 8;
  static final int TYPE_REGEXP = 9;
  static final int TYPE_UNKNOWN = 13;

  static int priority(dynamic a, {inList:false}){
    if(a == null) return TYPE_NULL;
    if(a is num) return TYPE_NUM;
    if(a is String) return TYPE_STRING;
    if(a is Map) return TYPE_MAP;
    if(a is List) {
      List la = a;
      return la.isEmpty && inList == false ? TYPE_EMPTY_LIST : TYPE_LIST;
    }
    if(a is bool) return TYPE_BOOL;
    if(a is DateTime) return TYPE_DATETIME;
    if(a is RegExp || a is Pattern) return TYPE_REGEXP;

    return TYPE_UNKNOWN;
  }
}