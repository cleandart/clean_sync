import 'package:clean_sync/server.dart';
import 'package:clean_data/clean_data.dart';

class ValidationException implements Exception {
  final String error;
  final String stackTrace;
  ValidationException(this.error, [this.stackTrace]);
  String toString() => error;
}

class ServerOperation {
  String name;
  List<Function> before;
  Function operation;
  List<Function> after;

  ServerOperation(this.name, {this.before, this.operation, this.after});

  ClientOperation toClientOperation() =>
      new ClientOperation(this.name, operation:this.operation);
}

class ClientOperation {
  String name;
  Function operation;

  ClientOperation(this.name, {this.operation});
}

// First element is ServerOperation, second is equivalent ClientOperation
List<List> incompatibleOperations = [
  [
    new ServerOperation('add',
      before: [(args, {user, MongoProvider collection}) {
        if (args is! Map) throw new ValidationException("Added document should be a Map");
        if (!args.containsKey("_id")) throw new ValidationException("Document does not contain _id");
        try {
          // There should be no document with given _id in collection, so this should throw
          collection.find({"_id":args["_id"]}).findOne();
          // As it gets here, collection found document with given _id
          throw new ValidationException("_id given is already used");
        } catch (e) {
          if (e is ValidationException) throw e;
        }
      }],
      operation: (args, {MongoProvider collection}) {
        collection.add(args, '');
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
        try {
          collection.find({"_id":args["_id"]}).findOne();
        } catch (e) {
          // Find one threw => there are no entries with given _id
          throw new ValidationException("No document with given _id found");
        }
      }],
      operation: (args, {MongoProvider collection}) {
        collection.remove(args["_id"], "");
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