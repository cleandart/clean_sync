import 'package:clean_sync/server.dart';
import 'package:clean_data/clean_data.dart';
import 'dart:async';

Function reduceArguments(Function op, docs, Map args, user, colls, caller) {
  if (op == null) return () => caller(() => null);
  if (user == null) {
    if (docs == null) {
      if (colls == null) return () => caller(() => op(args));
      else return () => caller(() => op(args, collection: colls));
    } else {
      if (colls == null) return () => caller(() => op(args, fullDocs: docs));
      else return () => caller(() => op(args, collection: colls, fullDocs: docs));
    }
  } else {
    if (docs == null) {
      if (colls == null) return () => caller(() => op(args, user: user));
      else return () => caller(() => op(args, collection: colls));
    } else {
      if (colls == null) return () => caller(() => op(args, fullDocs: docs, user: user));
      else return () => caller(() => op(args, collection: colls, fullDocs: docs, user: user));
    }
  }
}

Function reduceArgumentsSync(Function op, docs, Map args, user, colls)
  => reduceArguments(op, docs, args, user, colls, (f) => f());


Function reduceArgumentsAsync(Function op, docs, Map args, user, colls)
  => reduceArguments(op, docs, args, user, colls, (f) => new Future(() => f()));

class ValidationException implements Exception {
  final String error;
  final String stackTrace;
  ValidationException(this.error, [this.stackTrace]);
  String toString() => error;
}

class ServerOperation {
  String name;
  List<Function> _before = [];
  Function operation;
  List<Function> _after = [];

  List<Function> get before {
    if (_before == null) _before = [];
    return _before;
  }

  List<Function> get after {
    if (_after == null) _after = [];
    return _after;
  }

  ServerOperation(this.name, {before, this.operation, after}) {
    _before = before;
    _after = after;
  }

  ClientOperation toClientOperation() =>
      new ClientOperation(this.name, operation:this.operation);
}

class ClientOperation {
  String name;
  Function operation;
  List<Function> _argsDecorator = [];

  List<Function> get argsDecorator {
    if (_argsDecorator == null) _argsDecorator = [];
    return _argsDecorator;
  }

  ClientOperation(this.name, {this.operation});
}

// First element is ServerOperation, second is equivalent ClientOperation
List<List> incompatibleOperations = [
  [
    new ServerOperation('add',
      before: [(args, {user, MongoProvider collection}) {
        if (args is! Map) throw new ValidationException("Added document should be a Map");
        if (!args.containsKey("_id")) throw new ValidationException("Document does not contain _id");
        // There should be no document with given _id in collection, so this should throw
        return collection.find({"_id":args["_id"]}).findOne().then((_) =>
        // As it gets here, collection found document with given _id
        throw new ValidationException("_id given is already used"))
        .catchError((e,s) {
          if (e is ValidationException) throw e;
        });

      }],
      operation: (args, {MongoProvider collection}) {
        return collection.add(args, '');
      }),

    new ClientOperation('add',
      operation: (args, {DataSet collection}) {
        collection.add(args);
      })
  ],
  [
    new ServerOperation('remove',
      before: [(args, {user, MongoProvider collection}) {
        if (args is! Map) throw new ValidationException("Args should be Map containing _id");
        if (!args.containsKey("_id")) throw new ValidationException("Args should contain _id");
        return collection.find({"_id":args["_id"]}).findOne()
            .catchError((e,s) =>
                // Find one threw => there are no entries with given _id
          throw new ValidationException("No document with given _id found"));

      }],
      operation: (args, {MongoProvider collection}) {
        return collection.remove(args["_id"], "");
      }),

    new ClientOperation('remove',
      operation: (args, {DataSet collection}){
        collection.remove(args["_id"]);
      })
  ]
];

List<ServerOperation> commonOperations = [
  new ServerOperation('change',
    before: [(args, {user, fullDocs}) {
      if (fullDocs is List) throw new ValidationException("Only one document at a time can be changed");
      if (args is! Map) throw new ValidationException("Args should be Map - see ChangeSet.toJson");
      if (args.containsKey("_id")) throw new ValidationException("Cannot change _id of document");
    }],
    operation: (args, {fullDocs}) {
      applyJSON(args,fullDocs);
    })
];