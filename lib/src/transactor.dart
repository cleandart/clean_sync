part of clean_sync.client;

// Should have the same registered operations as MongoServer
// Should apply changes to local collection

class Transactor {
  Connection _connection;
  DataReference<bool> updateLock;
  String author;
  IdGenerator _idGenerator;

  Map<String, ClientOperation> operations = {};

  Transactor(this._connection, this.updateLock, this.author, this._idGenerator) {
    defaultOperations.commonOperations.forEach((o) => operations[o.name] = o.toClientOperation());
    defaultOperations.incompatibleOperations.forEach((o) => operations[o[1].name] = o[1]);
    registerArgsDecorator("add", (args) {
       if (!args.containsKey("_id")) args["_id"] = _idGenerator.next();
    });
  }

  Transactor.config(this._connection, this.updateLock, this.author, this._idGenerator);

  Future operation(String name, Map args, {List<DataMap> docs, List<List> colls}) {
    operations[name].argsDecorator.forEach((f) => f(args));
    performClientOperation(name, args, docs: docs, colls: colls, shouldDecorateArgs: false);
    return performServerOperation(name, args, docs: docs, colls: colls, shouldDecorateArgs: false);

  }

  Future performServerOperation(String name, Map args, {docs, colls, shouldDecorateArgs: true}){
    print('server operation: ${name}');
    if (shouldDecorateArgs) operations[name].argsDecorator.forEach((f) => f(args));
    List<String> serverColls;
    List<List<String>> serverDocs;


    if (colls == null) {
      serverColls = [];
    } else {
      serverColls = new List.from(colls.map((e) => e[1]));
    }

    if (docs == null) {
      serverDocs = [];
    } else {
      serverDocs = new List.from(docs.map((e) => [e['_id'], e['__clean_collection']]));
    }

    return  _connection.send(() {
      return new ClientRequest('sync-operation', {
        'operation': name,
        'args': args,
        'docs': serverDocs,
        'colls': serverColls,
        'author': this.author,
        'clientVersion': this._idGenerator.next()
      });
    });

  }

  performClientOperation(String name, Map args, {docs, colls, shouldDecorateArgs: true}) {
    List<DataSet> clientColls = new List.from(colls.map((e) => e[0]));
    ClientOperation op = operations[name];
    if (shouldDecorateArgs) op.argsDecorator.forEach((f) => f(args));
    updateLock.value = true;
    op.operation(args, colls: clientColls, docs: docs);
    updateLock.value = false;
  }

  registerClientOperation(name, {operation}){
    logger.fine("registering operation $name");
    operations[name] = new ClientOperation(name, operation: operation);
  }

  registerArgsDecorator(operationName, argsDecorator) {
    operations[operationName].argsDecorator.add(argsDecorator);
  }

}