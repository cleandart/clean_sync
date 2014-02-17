
part of clean_sync.server;

class Entry {
  DateTime timeAdded;
  DateTime expirationDate;
  dynamic value;

  Entry(this.value, this.timeAdded, this.expirationDate){
  }
}

typedef Future ValueGenerator();

class Cache {
  Map m;
  Duration _timeOut;
  LinkedHashMap<dynamic, Entry> entries = {};

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

  put(key, val){
    var timeAdded = new DateTime.now();
    var expirationDate = timeAdded.add(_timeOut);
    entries[key] = new Entry(val, timeAdded, expirationDate);
  }


  Future putIfAbsent(key, ValueGenerator val){
    clear();
    if (entries.containsKey(key)) {
      return new Future.value(entries[key].value);
    } else {
      return val().then((value){
        put(key, value);
        return value;
      });
    }
  }

  Entry getEntry(key){
    clear();
    return entries[key];
  }

  get(key){
    Entry res = getEntry(key);
    if (res == null) {
      return res;
    } else {
      return res.value;
    }
  }

}