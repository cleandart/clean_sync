part of clean_sync.client;

// Should have the same registered operations as MongoServer
// Should apply changes to local collection

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

  Future performServerOperation(String name, Map args, {docs, collection}){
    Completer completer = new Completer();
    queue.add({
      "operation": () => _performServerOperation(name, args, docs:docs, collection:collection),
      "completer": completer
    });
    _performOne();
    return completer.future;
  }
  /**
   * [args] should contain 'collection' of type List<[DataSet, name]>,
   * List of full documents 'docs' and Map of 'args'.
   * Performs operation specified in [name] on data given and sends
   * minimized operation data to the server
   */
  Future performOperation(String name, Map args, {docs, collection}) {
    Completer completer = new Completer();
    queue.add({
      "operation": () => _performOperation(name, args, docs:docs, collection: collection),
      "completer": completer
    });
    _performOne();
    return completer.future;
  }

  performClientOperation(String name, Map args, {docs, collection, decorateArgs: true}) {
    ClientOperation op = operations[name];
    if (decorateArgs) op.argsDecorator.forEach((f) => f(args));
    // collection is List: [DataSet, name]
    var fullColls = null;
    if (collection != null) {
      if (collection[0] is List) {
        fullColls = collection.map((e) => e[0]).toList();
      } else fullColls = collection[0];
    }
    var fullDocs = docs;

    updateLock.value = true;
    reduceArgumentsSync(op.operation, fullDocs, args, null, fullColls)();
    updateLock.value = false;
  }

  Future _performServerOperation(String name, Map args, {docs, collection, decorateArgs: true}){
    if (decorateArgs) operations[name].argsDecorator.forEach((f) => f(args));
    String clientVersion = _idGenerator.next();
    var collectionNames;
    return new Future(() => _connection.send(() {
    // collection could be [], [[]] or null if not specified
      if (collection == null) collection = [];
      else if (collection[0] is List) {
        collectionNames = collection.map((e) => e[1]).toList();
      } else collectionNames = collection[1];
      // if not specified, collection is now null
      // docs could be List<Map>, Map if specified, [] or null if not specified
      List newDocs;
      if (docs is Map)
        newDocs = [docs["_id"], docs["__clean_collection"]];
      else
      if (docs is List) {
        if (docs.isEmpty) newDocs = null;
        else {
          print(docs);
          newDocs = docs.map((e) => [e["_id"], e["__clean_collection"]]).toList();
        }
      }
      return new ClientRequest('sync-operation', {
        'operation': name,
        'docs': newDocs,
        'collection': collectionNames,
        'args': args,
        'author': author,
        'clientVersion': clientVersion
      });
    }));
  }

  Future _performOperation(String name, Map args, {docs, collection}) {
    performClientOperation(name, args, docs:docs, collection:collection, decorateArgs: false);
    return _performServerOperation(name, args, docs: docs, collection: collection, decorateArgs: false);
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
    operations[operationName].argsDecorator.add(argsDecorator);
  }

}