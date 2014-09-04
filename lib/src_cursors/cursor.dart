part of clean_cursors;

class Cursor {
  List path;
  Reference reference;

  get value => reference.lookupIn(path);
  set value(newValue) => reference.changeIn(path, newValue);

  Cursor(this.reference, Iterable path) {
    this.path = new List.from(path);
  }

  Stream get onChange {
    if(_onChangeController == null) {
      _onChangeController = new StreamController.broadcast();
      reference.listenIn(path, _onChangeController, false);
    }
    return _onChangeController.stream;
  }

  Stream get onChangeSync  {
    if(_onChangeSyncController == null) {
      _onChangeSyncController = new StreamController.broadcast(sync: true);
      reference.listenIn(path, _onChangeSyncController, true);
    }
    return _onChangeSyncController.stream;
  }

  StreamController _onChangeController;
  StreamController _onChangeSyncController;

  dispose() {
    if(_onChangeController != null)
      _onChangeController.close().then((reference.stopListenIn(path, _onChangeController, false)));
    if(_onChangeSyncController != null)
      _onChangeSyncController.close().then((reference.stopListenIn(path, _onChangeSyncController, true)));
  }
}

abstract class DeepCursor extends Cursor {
  List _pathForF;

  DeepCursor(Reference reference, Iterable path) : super(reference, path) {
    _pathForF = new List(path.length + 1);
   for(int i=0;i < path.length; i++) _pathForF[i] = this.path[i];
  }

  _lookup(key) {
    _pathForF[_pathForF.length - 1] = key;
    return reference.lookupIn(_pathForF);
  }

  _change(key, value) {
    _pathForF[_pathForF.length - 1] = key;
    return reference.changeIn(_pathForF, value);
  }

  _remove(key) {
    _pathForF[_pathForF.length - 1] = key;
    return reference.removeIn(_pathForF);
  }
}
