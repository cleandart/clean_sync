part of clean_sync.server;

Logger _profilingLogger = new Logger('clean_sync.profiling');

Map watches = {};
var watchID = 0;

num startWatch(identifier) {
  watchID++;
  watches[watchID] = [new Stopwatch()..start(), identifier];
  _profilingLogger.finer('$watchID Started processing request ($identifier).');
  return watchID;
}
stopWatch(watchID) {
  Stopwatch watch = watches[watchID][0];
  var identifier = watches[watchID][1];
  var logging_fn = watch.elapsedMilliseconds > 100 ? _profilingLogger.warning : _profilingLogger.finer;
  logging_fn('$watchID Processing request ($identifier) took ${watch.elapsed}.');
  watch.stop();
  watches.remove(watchID);
}

logElapsedTime(watchID) {
  var watch = watches[watchID][0];
  var identifier = watches[watchID][1];
  _profilingLogger.finer('$watchID Processing request ($identifier) currently elapsed '
              '${watch.elapsed}.');
}
