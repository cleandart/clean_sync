library json;

// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

import 'dart:math';
import 'package:persistent/persistent.dart';
import 'package:clean_sync/clean_cursors.dart';

final String CLEAN_UNDEFINED = '__clean_undefined';

/**
 * Applies json ChangeSet to [CleanSet], [CleanData], [CleanList].
 * If cleanData is DataSet, index on '_id' must be set.
 */

void applyJSON(Map jsonChangeSet, cleanData) {
  if(cleanData is Set) {
   jsonChangeSet.forEach((key, change) {
     findById(key) {
       if(cleanData is SetCursor)
         return cleanData.findBy('_id', key).single;
       else
         return cleanData.singleWhere((E) => E['_id'] == key);
     };
     if(change is List) {
       if(change[0] != CLEAN_UNDEFINED)
         cleanData.remove(findById(key));
       if(change[1] != CLEAN_UNDEFINED)
        cleanData.add(change[1]);
     }
     else
       applyJSON(change, findById(key));
   });
  }
  else if(cleanData is Map) {
   jsonChangeSet.forEach((key, change) {
     if(change is List) {
       if (change[1] != CLEAN_UNDEFINED) {
         cleanData[key] = change[1];
       } else {
           cleanData.remove(key);
       }
     }
     else applyJSON(change, cleanData[key]);
   });
  }
  else if(cleanData is List) {
   jsonChangeSet.forEach((key, change) {
     key = int.parse(key);
     if(change is List) {
       if (change[1] == CLEAN_UNDEFINED) {
         if(change[0] != CLEAN_UNDEFINED) cleanData.removeLast();
       } else if(change[0] == CLEAN_UNDEFINED) {
         cleanData.add(change[1]);
       }
       else {
         cleanData[key] = change[1];
       }
     }
     else applyJSON(change, cleanData[key]);
   });
  }
}

calcJSONDiff(ne, old) {
  //print('Newo $ne, old $old');
  if(ne is PersistentMap  && old is PersistentMap) {
   var diff = {};
   ne.keys.forEach((k) {
     if(!old.containsKey(k)) {
       diff[k] = [CLEAN_UNDEFINED, depersistent(ne[k])];
     }
     else {
       var d = calcJSONDiff(ne[k], old[k]);
       if(d != null) diff[k] = d;
     }
   });
   old.keys.forEach((k) {
     if(!ne.containsKey(k)) {
       diff[k] = [depersistent(old[k]), CLEAN_UNDEFINED];
     }
   });
   if(diff.isEmpty) return null;
   else return diff;
  }
  else if(ne is List && old is List) {
   var diff = {};
   for(int i=0; i < max(ne.length, old.length); i++) {
     if(i < ne.length && i < old.length) {
       var d = calcJSONDiff(ne[i], old[i]);
       if(d != null) diff['$i'] = d;
     } else if(i < ne.length) {
       diff['$i'] = [CLEAN_UNDEFINED, ne[i]];
     }
     else {
       diff['$i'] = [depersistent(old[i]), CLEAN_UNDEFINED];
     }
   }
   if(diff.isEmpty) return null;
   else return diff;
  }
  else if(ne is SetCursor && old is SetCursor) {
   var diff = {};
   ne.forEach((k) {
     if(old.findBy('_id', k['_id']).isEmpty) {
       diff[k['_id']] = [CLEAN_UNDEFINED, depersistent(k)];
     }
     else {
       var d = calcJSONDiff(k, old.findBy('_id', k['_id']).first);
       if(d != null) diff[k['_id']] = d;
     }
   });
   old.forEach((k) {
     if(ne.findBy('_id', k['_id']).isEmpty) {
       diff[k['_id']] = [depersistent(k), CLEAN_UNDEFINED];
     }
   });
   if(diff.isEmpty) return null;
   else return diff;
  }
  else {
   if(ne != old) return [depersistent(old), depersistent(ne)];
   return null;
  }
}

depersistent(a) {
  if(a is PersistentMap) {
    Map map = {};
    a.keys.forEach((e) => map[e] = depersistent(a.lookup(e)));
    return map;
  }
  else if(a is PersistentVector) {
    return a.map((e)=> depersistent(e)).toList();
  }
  else return a;
}