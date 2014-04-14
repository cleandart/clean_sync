part of clean_sync.client;

// Should have the same registered operations as MongoServer
// Should apply changes to local collections

class ServerOperation {
  String name;
  Function before;
  Function operation;
  Function after;

  ServerOperation(this.name, {this.operation, this.before,
      this.after});
}

class Transactor {
  Connection _connection;
  // Local collections - Map: {name: [String], data: [Collection]}

  Map<String, ServerOperation> operations = {};

  Transactor(this._connection);

  Future operation(String name, Map args) {
    performClientOperation(name, args);
    return new Future(() => _connection.send(() {
        if (args["collections"][0] is List) {
          args["collections"] = args["collections"].map((e) => e[1]).toList();
        } else args["collections"] = args["collections"][1];
        args["docs"] = args["docs"].map((e) => [e["_id"], e["__clean_collection"]]).toList();
        return new ClientRequest('sync', {'action':'operation', 'name' : name, 'args':args});
      }));
  }

  performClientOperation(String name, Map args) {
    ServerOperation op = operations[name];
    // collections is List: [DataSet, name]
    var fullColls = null;
    if (args["collections"] != null) {
      if (args["collections"][0] is List) {
        fullColls = args["collections"].map((e) => e[0]);
      } else fullColls = args["collections"][0];
    }
    var fullDocs = args["docs"];

    if (op.operation != null) op.operation(fullDocs, args["args"], fullColls);

  }

  registerOperation(name, {operation, before, after}){
    logger.fine("registering operation $name");
    // We don't need before nor after operations here
    operations[name] = new ServerOperation(name, operation: operation,
        before: null, after: null);
  }

}