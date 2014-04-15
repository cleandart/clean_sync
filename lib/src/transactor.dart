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

  Map<String, ServerOperation> operations = {};

  Transactor(this._connection);

  /**
   * [args] should contain 'collections' of type List<[DataSet, name]>,
   * List of full documents 'docs' and Map of 'args'.
   * Performs operation specified in [name] on data given and sends
   * minimized operation data to the server
   */
  Future operation(String name, Map args) {
    performClientOperation(name, args);
    return new Future(() => _connection.send(() {
      // collections could be [], [[]] or null if not specified
        if ((args["collections"] == null) || (args["collections"].isEmpty)) args["collections"] = [null,null];
        else if (args["collections"][0] is List) {
          if (args["collections"][0].isEmpty) args["collections"] = [null,null];
          args["collections"] = args["collections"].map((e) => e[1]).toList();
        } else args["collections"] = args["collections"][1];
        // if not specified, collections is now null
        // docs could be List<Map>, Map if specified, [] or null if not specified
        if (args["docs"] is Map)
          args["docs"] = {"_id":args["docs"]["_id"], "__clean_collection": args["docs"]["__clean_collection"]};
        if (args["docs"] is List) {
          if (args["docs"].isEmpty) args["docs"] = null;
          else args["docs"] = args["docs"].map((e) => [e["_id"], e["__clean_collection"]]).toList();
        }
        return new ClientRequest('sync-operation', {'operation' : name, 'args':args});
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