part of clean_cursors;

class MapCursor<K, V> extends DeepCursor implements Map {

  MapCursor(Reference ref, List path) : super(ref, path);

  MapCursor.from(Map map) : super(new Reference.from(map), []);

  MapCursor.empty() : super(new Reference.from({}), []);

  PersistentMap get value => super.value;
  set value(newValue) => reference.changeIn(path, newValue);

  V operator[](key) {
    _pathForF[_pathForF.length - 1] = key;
    return reference.cursorForIn(_pathForF, forPrimitives: false);
  }

  /**
   * Returns true if there is no {key, value} pair in the data object.
   */
  bool get isEmpty {
    return value.isEmpty;
  }

  /**
   * Returns true if there is at least one {key, value} pair in the data object.
   */
  bool get isNotEmpty => value.isNotEmpty;

  /**
   * The keys of data object.
   */
  Iterable<K> get keys {
    return value.keys;
  }
  /**
   * The values of [DataMap].
   */
  Iterable<V> get values {
    return value.values;
  }

  /**
   * The number of {key, value} pairs in the [DataMap].
   */
  int get length {
    return value.length;
  }

  /**
   * Returns whether this data object contains the given [key].
   */
  bool containsKey(K key) {
    return value.containsKey(key);
  }

  bool containsValue(Object value) {
    bool contains = false;
    this.value.forEachKeyValue((K, elem) { if(elem == value) contains = true;});
    return contains;
  }

  /**
   * Assigns the [value] to the [key] field.
   */
  void add(K key, V value) {
    _addAll({key: value});
  }

  /**
   * Adds all key-value pairs of [other] to this data.
   */
  void addAll(Map<K, V> other) {
    _addAll(other);
  }

  void _addAll(Map<K, V> other) {
    other.forEach((key, value) {
      this._change(key, value);
    });
  }

  /**
   * Assigns the [value] to the [key] field.
   */
  void operator[]=(K key, V value) {
    _addAll({key: value});
  }

  /**
   * Removes [key] from the data object.
   */
  void remove(K key) {
    return _removeAll([key]);
  }

  /**
   * Remove all [keys] from the data object.
   */
  void removeAll(List<K> keys) {
    return _removeAll(keys);
  }


  void _removeAll(List<K> keys) {
    for (var key in keys) {
      this._remove(key);
    }
  }

  void clear() {
    _removeAll(keys.toList());
  }

  void forEach(void f(K key, V value)) {
    value.forEachKeyValue(f);
  }

  Cursor ref(K key) {
    _pathForF[_pathForF.length - 1] = key;
    return reference.cursorForIn(_pathForF, forPrimitives: true);
  }

  putIfAbsent(K key, ifAbsent()) {
    if (!containsKey(key)) {
      _addAll({key: ifAbsent()});
    }
  }

  /**
   * Converts to Map.
   */
  Map toJson() => new Map.fromIterables(this.keys, this.values);

  /**
   * Returns Json representation of the object.
   */
  String toString() => 'MapCursor(${toJson().toString()})';
}