part of clean_sync.server;

// helper functions for profiling

Logger _profilingLogger = new Logger('clean_sync.profiling');

Map _watches = {};
var _watchID = 0;

num _startWatch(identifier) {
  _watchID++;
  _watches[_watchID] = [new Stopwatch()..start(), identifier];
  _profilingLogger.finer('$_watchID Started processing request ($identifier).');
  return _watchID;
}
_stopWatch(watchID) {
  Stopwatch watch = _watches[watchID][0];
  var identifier = _watches[watchID][1];
  var logging_fn = watch.elapsedMilliseconds > 100 ? _profilingLogger.warning : _profilingLogger.finer;
  logging_fn('$watchID Processing request ($identifier) took ${watch.elapsed}.');
  watch.stop();
  _watches.remove(watchID);
}

_logElapsedTime(watchID) {
  var watch = _watches[watchID][0];
  var identifier = _watches[watchID][1];
  _profilingLogger.finer('$watchID Processing request ($identifier) currently elapsed '
              '${watch.elapsed}.');
}
