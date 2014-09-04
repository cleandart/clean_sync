part of clean_cursors;

class ListCursor<V> extends DeepCursor implements List {


  ListCursor(Reference ref, List path) : super(ref, path);

  ListCursor.from(List list) : super(new Reference.from(list), []);

  ListCursor.empty() : super(new Reference.from([]), []);

  PersistentVector get value => super.value;
  set value(newValue) => reference.changeIn(path, newValue);

  V operator[](int key) => elementAt(key);

  V elementAt(int index) {
    _pathForF[_pathForF.length - 1] = index;
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
   * The number of {key, value} pairs in the [DataMap].
   */
  int get length {
    return value.length;
  }

  set length(int len) {
    PersistentVector v = value;
    value = v.withTransient((TransientVector tv) {
      while(tv.length > len) tv.doPop();
      while(tv.length < len) tv.doPush(null);
    });
    assert(value.length == len);
  }

  bool contains(V key) {
    return value.contains(key);
  }

  void add(V value) {
    _addAll([value]);
  }

  void addAll(Iterable<V> other) {
    _addAll(other);
  }

  void _addAll(Iterable<V> other) {
    other.forEach((value) {
      if (value is! Persistent) {
        value = deepPersistent(value);
      }
      this._change(this.length, value);
    });
  }

  void operator[]=(int key, V value) {
    this._change(key, value);
  }

  void set(int index, V value) {
    this._change(index, value);
  }

  void pop() {
    this._remove(this.length - 1);
  }

  void forEach(void f(V value)) {
    value.forEach(f);
  }

  Cursor ref(int key) {
    _pathForF[_pathForF.length - 1] = key;
     return reference.cursorForIn(_pathForF, forPrimitives: true);
  }

  Iterable map(f(V value)) =>
    value.map(f);

  Iterator get iterator => value.iterator;

  bool any(f) => value.any(f);

  /**
   * Converts to Map.
   */

  List toJson() => value.toList();

  /**
   * Returns Json representation of the object.
   */
  String toString() => 'ListCursor(${toJson().toString()})';

  noSuchMethod(Invocation invocation) {
    return super.noSuchMethod(invocation);
  }
}