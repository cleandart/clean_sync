
part of clean_sync.server;

class Entry {
  DateTime timeAdded;
  DateTime expirationDate;
  dynamic value;

  Entry(this.value, this.timeAdded, this.expirationDate){
  }
}

class Cache {
  Duration _timeOut;
  num _capacity;
  LinkedHashMap<dynamic, Entry> _entries = {};

  Cache(this._timeOut, this._capacity);

  _removeFirst(){
    _entries.remove(_entries.keys.first);
  }

  invalidate() => _entries.clear();

  clear(){
    var toRemove = [];
    for(var k in _entries.keys){
      if ((new DateTime.now()).isAfter(_entries[k].expirationDate)) {
        toRemove.add(k);
      } else {
        break;
      }
    }
    toRemove.forEach((e){_entries.remove(e);});
    while(_entries.length > _capacity) {
      _removeFirst();
    }
  }

  put(key, val){
    var timeAdded = new DateTime.now();
    var expirationDate = timeAdded.add(_timeOut);
    _entries[key] = new Entry(val, timeAdded, expirationDate);
    clear();
  }


  Future putIfAbsent(key, val()){
    clear();
    if (_entries.containsKey(key)) {
      Entry value = _entries[key];
      return new Future.delayed(new Duration(), () => value.value);
    } else {
        return new Future.sync(() => val())
          .then((value){
          put(key, value);
          return value;
      });
    }
  }

  Entry getEntry(key){
    clear();
    return _entries[key];
  }

  get(key){
    Entry res = getEntry(key);
    if (res == null) {
      return null;
    } else {
      return res.value;
    }
  }

}

class DummyCache implements Cache {
  const DummyCache();
  put(key, val) => null;
  putIfAbsent(key, val()) => new Future.value(val());
  getEntry(key) => null;
  get(key) => null;
  clear(){}
  invalidate(){}
  noSuchMethod(inv) => super.noSuchMethod(inv);
}

const dummyCache = const DummyCache();