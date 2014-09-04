part of clean_cursors;


class NoIndexException implements Exception {
  String cause;
  NoIndexException(this.cause);
}


class SetCursor extends Cursor with IterableMixin {
  MapCursor mapCursor;
  var modifyBeforeAdd;

  factory SetCursor() => new SetCursor.fromRef(new Reference.from({}));
  SetCursor.fromRef(Reference ref, {this.modifyBeforeAdd}) : super(ref, []) {
    mapCursor = new MapCursor(ref, []);
  }

  factory SetCursor.from(Iterable data) {
    Map map = {};
    data.forEach((e) => map[e['_id']] = e);
    return new SetCursor.fromRef(new Reference.from(map));
  }

  Iterable findBy(String property, dynamic value) {
    if(!indexes.contains(property))
      throw new NoIndexException('Property $property is not indexed.');
    if(property == '_id') {
      if(mapCursor.containsKey(value)) {
        return [mapCursor.ref(value)];
      }
      else
        return [];
    }
    else {
      return mapCursor.values.
          where((e) => e.containsKey(property) && e[property] == value).
          map((e) => mapCursor.ref(e['_id']));
      }
  }

  List indexes = [];

  void addIndex([Iterable<String> indexedProps]) {
    indexes.addAll(indexedProps);
  }

  remove(Map obj) {
    mapCursor.remove(obj['_id']);
  }

  removeBy(String property, dynamic value) {
    findBy(property, value).forEach((e) => remove(e));
  }

  add(Map obj) {
    obj = modifyBeforeAdd != null ? modifyBeforeAdd(obj) : obj;
    mapCursor.add(obj['_id'], obj);
  }

  addAll(Iterable toAdd) {
    reference.changeIn(path, (this.value as PersistentMap).withTransient((TransientMap map) {
      toAdd.forEach((add) {
        if(modifyBeforeAdd != null) add = modifyBeforeAdd(add);
        map.doInsert(add['_id'], deepPersistent(add));
      });
    }));
  }

  clear() {
    reference.changeIn([], {});
  }

  Iterator get iterator =>
      mapCursor.values
        .map((PersistentMap map) => reference.cursorFor(map['_id'])).iterator;

  String toString() => 'SetCursor(${mapCursor.toJson().toString()})';
}