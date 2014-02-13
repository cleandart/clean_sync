
import 'dart:collection';

class Entry {
  num timeAdded;
  dynamic value;
}

class Cache {
  Map m;
  num _timeOut;
  LinkedHashMap<dynamic, Entry> entries;

  Cache(this._timeOut);

  clear(){
    for(var k in entries.keys){
      if (entries[k].timeAdded) {

      }
    }
  }


  save(key, val){

  }
}