
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
  num capacity;
  LinkedHashMap<dynamic, Entry> entries = {};

  Cache(this._timeOut, this.capacity);

  _removeFirst(){
    entries.remove(entries.keys.first);
  }

  clear(){
    var toRemove = [];
    for(var k in entries.keys){
      if ((new DateTime.now()).isAfter(entries[k].expirationDate)) {
        toRemove.add(k);
      }
    }
    while(entries.length > capacity) {
      _removeFirst();
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
//      print('hit');
      return new Future.delayed(new Duration(), () => entries[key].value);
    } else {
//      print('miss');
      return val().then((value){
        put(key, value);
        return value;
      });
    }
    if (entries.length > capacity) {
      _removeFirst();
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

class DummyCache implements Cache {
  const DummyCache();
  put(key, val) => null;
  putIfAbsent(key, ValueGenerator val) => val();
  getEntry(key) => null;
  get(key) => null;
  clear(){}
}

const dummyCache = const DummyCache();