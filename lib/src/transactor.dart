part of clean_sync.client;

// Should have the same registered operations as MongoServer
// Should apply changes to local collections

class Transactor {
  Connection _connection;

  Map<String, ClientOperation> operations = {};

  Transactor(this._connection) {
    defaultOperations.commonOperations.forEach((o) => operations[o.name] = o.toClientOperation());
    defaultOperations.incompatibleOperations.forEach((o) => operations[o[1].name] = o[1]);
  }

  Transactor.config(this._connection);

  Future operation(String name, Map args, {List<DataMap> docs, List<List> colls}) {
    List<DataSet> clientColls = new List.from(colls.map((e) => e[0]));
    List<String> serverColls = new List.from(colls.map((e) => e[1]));
    List<List<String>> serverDocs = new List.from(docs.map((e) => [e['_id'], e['__clean_collection']]));
    performClientOperation(name, args, docs: docs, colls: clientColls);
    return _connection.send(() {
      return new ClientRequest('sync-operation', {
        'operation': name,
        'args': args,
        'docs': serverDocs,
        'colls': serverColls,
        'author': args["author"],
        'clientVersion': args["clientVersion"]
      });
    });
  }

  Future performClientOperation(String name, Map args, {docs, colls}) {
//    print(colls);
    ClientOperation op = operations[name];
    return op.operation(args, docs: docs, colls: colls);
  }

  registerClientOperation(name, {operation}){
    logger.fine("registering operation $name");
    operations[name] = new ClientOperation(name, operation: operation);
  }

}