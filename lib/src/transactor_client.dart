part of clean_sync.client;

// Should have the same registered operations as MongoServer
// Should apply changes to local collection

/// Representation of client operation call.
class ClientOperationCall extends CommonOperationCall {
  String name;
  List<Map> docs;
  List<DataSet> colls;
  Map args;
  String author;
  ClientOperationCall(this.name, {this.docs, this.colls, this.args, this.author});
}

/// Client endpoints for performing transactions.
class TransactorClient {
  Connection _connection;
  DataReference<bool> updateLock;
  String author;
  IdGenerator _idGenerator;
  bool operationPerformed = false;

  Map<String, ClientOperation> operations = {};

  TransactorClient(this._connection, this.updateLock, this.author, this._idGenerator) {
    defaultOperations.commonOperations.forEach((o) => operations[o.name] = o.toClientOperation());
    clientOperations.operations.forEach((o) => operations[o.name] = o);
    registerArgsDecorator("add", (args) {
       if (!args.containsKey("_id")) args["_id"] = _idGenerator.next();
    });
  }

  TransactorClient.config(this._connection, this.updateLock, this.author, this._idGenerator);

  /// Performs both client and server operation.
  Future operation(String name, Map args, {List<DataMap> docs, List<Subscription> subs}) {
    if(operations[name] == null) {
      _logger.shout('(transcator) Operation "$name" not found!!!');
      throw new Exception('Operation "$name" not found!!!');
    }
    operationPerformed = true;

    operations[name].argsDecorator.forEach((f) => f(args));
    performClientOperation(name, args, docs: docs, subs: subs, shouldDecorateArgs: false);
    return performServerOperation(name, args, docs: docs, subs: subs, shouldDecorateArgs: false);
  }

  /// Sends serialized operation to server.
  Future performServerOperation(String name, Map args, {docs, List<Subscription> subs, shouldDecorateArgs: true}){
    if (shouldDecorateArgs) operations[name].argsDecorator.forEach((f) => f(args));
    List<String> serverColls;
    List<List<String>> serverDocs;

    if (subs == null) {
      serverColls = [];
    } else {
      serverColls = new List.from(subs.map((e) => e.mongoCollectionName));
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
    }).then((Map value) {
      if(value == null || !value.containsKey('result') ||
          value['result'] != 'ok')
          _logger.warning('Operation "$name" completed with error ($value)');
      else
        _logger.fine('Operation "$name": completed correctly');
      return value;
    });

  }

  /// Performs the operation on client.
  performClientOperation(String name, Map args, {docs, List<Subscription> subs, shouldDecorateArgs: true}) {
    List<DataSet> clientColls = subs != null ? new List.from(subs.map((e) => e.collection)) : null;
    ClientOperation op = operations[name];
    if (shouldDecorateArgs) op.argsDecorator.forEach((f) => f(args));
    updateLock.value = true;
    op.operation(new ClientOperationCall(name, args: args, colls: clientColls,
        docs: docs, author: this.author));
    updateLock.value = false;
  }

  registerClientOperation(name, {operation}){
    _logger.fine("registering operation $name");
    operations[name] = new ClientOperation(name, operation: operation);
  }

  registerArgsDecorator(operationName, argsDecorator) {
    operations[operationName].argsDecorator.add(argsDecorator);
  }

}
