
part of clean_sync.server;

class Entry {
  DateTime timeAdded;
  DateTime expirationDate;
  dynamic value;

  /// Representation of value with some additional meta-data needed by [Cache]
  /// to work.
  Entry(this.value, this.timeAdded, this.expirationDate){
  }
}

class Cache {
  Duration _timeOut;
  num _capacity;
  LinkedHashMap<dynamic, Entry> _entries = {};

  /// Cache for remembering key-value pairs. The key-value pairs are internally
  /// stored in a [Map] (which poses a restriction on the types of keys that are
  /// accepted).
  ///
  /// [_timeOut] specified how long each key-value pair stays in the cache,
  /// after this time it is considered to be invalid it is not returned by the
  /// [get] method.
  /// [_capacity] is the maximum number of items allowed in the cache. If more
  /// items are put to the cache, the oldest ones are deleted.
  Cache(this._timeOut, this._capacity);

  _removeFirst(){
    _entries.remove(_entries.keys.first);
  }

  /// Empties the cache, deleting all stored key-value pairs.
  invalidate() => _entries.clear();

  /// Clears all entries according to the specified [_timeOut] parameter.
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

  /// Save a new [key]-[val] pair.
  put(key, val){
    var timeAdded = new DateTime.now();
    var expirationDate = timeAdded.add(_timeOut);
    _entries[key] = new Entry(val, timeAdded, expirationDate);
    clear();
  }

  /// If [key] is not yet in cache, use [val] to obtain the value and save this
  /// key-value pair.
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

  /// Returns [Entry] mapped to [key].
  Entry getEntry(key){
    clear();
    return _entries[key];
  }

  /// Returns the value that was saved for [key] or [null] if the [key] is not
  /// present in cache.
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
  /// Simplest possible implementation of [Cache] that in fact does not cache
  /// anything (all values are calculated whenever they are needed). Useful for
  /// testing purposes, for example.
  const DummyCache();
  put(key, val) => null;
  putIfAbsent(key, val()) => new Future.value(val());
  getEntry(key) => null;
  get(key) => null;
  clear(){}
  invalidate(){}
}

/// Instance of [DummyCache].
const dummyCache = const DummyCache();
