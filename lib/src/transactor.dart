part of clean_sync.client;

// Should have the same registered operations as MongoServer
// Should apply changes to local collections

class Transactor {
  Connection _connection;
  bool running = false;
  DataReference updateLock;
  String author;
  IdGenerator _idGenerator;

  Map<String, ClientOperation> operations = {};

  List queue = [];

  Transactor(this._connection, this.updateLock, this.author, this._idGenerator) {
    ops.commonOperations.forEach((o) => operations[o.name] = o.toClientOperation());
    ops.incompatibleOperations.forEach((o) => operations[o[1].name] = o[1]);
    //TODO: if "_id" is present, do not override it
    registerArgsDecorator("add", (args) {
       if (!args.containsKey("_id")) args["_id"] = _idGenerator.next();
    });
  }

  Transactor.config(this._connection, this.updateLock, this.author, this._idGenerator);

  Future performServerOperation(String name, Map args, {docs, collections}){
    Completer completer = new Completer();
    queue.add({
      "operation": () => _performServerOperation(name, args, docs:docs, collections:collections),
      "completer": completer
    });
    _performOne();
    return completer.future;
  }
  /**
   * [args] should contain 'collections' of type List<[DataSet, name]>,
   * List of full documents 'docs' and Map of 'args'.
   * Performs operation specified in [name] on data given and sends
   * minimized operation data to the server
   */
  Future performOperation(String name, Map args, {docs, collections}) {
    Completer completer = new Completer();
    queue.add({
      "operation": () => _performOperation(name, args, docs:docs, collections: collections),
      "completer": completer
    });
    _performOne();
    return completer.future;
  }

  performClientOperation(String name, Map args, {docs, collections, decorateArgs: true}) {
    if (decorateArgs) operations[name].argsDecorator.forEach((f) => f(args));
    ClientOperation op = operations[name];
    // collections is List: [DataSet, name]
    var fullColls = null;
    if (collections != null) {
      if (collections[0] is List) {
        fullColls = collections.map((e) => e[0]).toList();
      } else fullColls = collections[0];
    }
    var fullDocs = docs;

    updateLock.value = true;
    reduceArgumentsSync(op.operation, fullDocs, args["args"], null, fullColls)();
    updateLock.value = false;
  }

  Future _performServerOperation(String name, Map args, {docs, collections, decorateArgs: true}){
    if (decorateArgs) operations[name].argsDecorator.forEach((f) => f(args));
    String clientVersion = _idGenerator.next();
    return new Future(() => _connection.send(() {
    // collections could be [], [[]] or null if not specified
      if ((collections == null) || (collections.isEmpty)) collections = [null,null];
      else if (collections[0] is List) {
        if (collections[0].isEmpty) collections = [null,null];
        collections = collections.map((e) => e[1]).toList();
      } else collections = collections[1];
      // if not specified, collections is now null
      // docs could be List<Map>, Map if specified, [] or null if not specified
      if (docs is Map)
        docs = {"_id":docs["_id"], "__clean_collection": docs["__clean_collection"]};
      if (docs is List) {
        if (docs.isEmpty) docs = null;
        else docs = docs.map((e) => [e["_id"], e["__clean_collection"]]).toList();
      }
      return new ClientRequest('sync-operation', {
        'operation': name,
        'docs': docs,
        'collections': collections,
        'args': args,
        'author': author,
        'clientVersion': clientVersion
      });
    }));
  }

  Future _performOperation(String name, Map args, {docs, collections}) {

    performClientOperation(name, args, docs:docs, collections:collections, decorateArgs: false);
    return _performServerOperation(name, args, docs: docs, collections: collections, decorateArgs: false);
  }

  _performOne() {
    if (running) return;
    if (queue.isEmpty) return;
    logger.fine('server: perform one');
    running = true;
    Map operation = queue.removeAt(0);
    operation["operation"]().then((result) {
      running = false;
      operation["completer"].complete(result);
      _performOne();
    });
  }

  registerClientOperation(name, {operation}){
    logger.fine("registering operation $name");
    // We don't need before nor after operations here
    operations[name] = new ClientOperation(name, operation: operation);
  }

  registerArgsDecorator(operationName, argsDecorator) {
    if (operations[operationName].argsDecorator == null)
      operations[operationName].argsDecorator = [argsDecorator];
    else operations[operationName].argsDecorator.add(argsDecorator);
  }

}