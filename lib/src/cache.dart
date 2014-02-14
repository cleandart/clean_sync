
import 'dart:collection';

class Entry {
  DateTime timeAdded;
  DateTime expirationDate;
  dynamic value;

  Entry(this.value, this.timeAdded, this.expirationDate){
  }
}

class Cache {
  Map m;
  Duration _timeOut;
  LinkedHashMap<dynamic, Entry> entries;

  Cache(this._timeOut);

  clear(){
    var toRemove = [];
    for(var k in entries.keys){
      if ((new DateTime.now()).isAfter(entries[k].expirationDate)) {
        toRemove.add(k);
      }
    }
    toRemove.forEach((e){entries.remove(e);});
  }

  save(key, val){
    var timeAdded = new DateTime.now();
    var expirationDate = timeAdded.add(_timeOut);

    entries[key] = new Entry(value);

  }
}