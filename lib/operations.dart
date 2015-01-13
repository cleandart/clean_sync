library clean_sync.operations;

import 'package:clean_data/clean_data.dart';
import 'package:logging/logging.dart';

Logger _logger = new Logger('mongo_wrapper_logger');

class ValidationException implements Exception {
  final String error;
  final String stackTrace;
  ValidationException(this.error, [this.stackTrace]);
  String toString() => error;
}

/// Common part of [ServerOperationCall] and [ClientOperationCall].
abstract class CommonOperationCall {
  String name;
  List docs;
  List colls;
  Map args;
}

/// Representation of operation on server together with validations and
/// callbacks.
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
    if(before != null)
      _before = before is List ? before : [before];
    else _before = [];

    if(after != null)
      _after = after is List ? after : [after];
    else _after = [];
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

List<ServerOperation> commonOperations = [
  new ServerOperation('change',
    before: (CommonOperationCall opCall) {
      if (opCall.args.containsKey("_id")) throw new ValidationException("Cannot change _id of document");
      return null;
    },
    operation: (CommonOperationCall opCall) {
      try {
        applyJSON(opCall.args, opCall.docs[0]);
      } catch (e, s){
        _logger.warning("could not apply change properly", e, s);
      }
    })
];