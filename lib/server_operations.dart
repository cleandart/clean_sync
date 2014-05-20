library clean_sync.server_operations;

import 'package:clean_sync/mongo_server.dart';
import 'package:logging/logging.dart';
import 'package:clean_sync/operations.dart';

Logger logger = new Logger('mongo_wrapper_logger');

// First element is ServerOperation, second is equivalent ClientOperation
List operations = [
    new ServerOperation('add',
      before: (ServerOperationCall opCall) {
        if (!opCall.args.containsKey("_id")) throw new ValidationException("Document does not contain _id");

        return opCall.colls[0].find({"_id": opCall.args["_id"]}).data()
            .then((data){
               if(data['data'].length > 0) {
                 throw new ValidationException("_id given is already used");
               }
               return null;
            })
        .catchError((e,s) {
          if (e is ValidationException) throw e;
        });
      },

      operation: (ServerOperationCall opCall) {
        return opCall.colls[0].add(opCall.args, opCall.author);
      }),

    new ServerOperation('remove',
      before: (ServerOperationCall opCall) {
        if (!opCall.args.containsKey("_id")) throw new ValidationException("Args should contain _id");
        return opCall.colls[0].find({"_id": opCall.args["_id"]}).findOne()
            .then((_) => null)
            .catchError((e,s) =>
                // Find one threw => there are no entries with given _id
          throw new ValidationException("No document with given _id found"));

      },
      operation: (ServerOperationCall opCall) {
        return opCall.colls[0].remove(opCall.args["_id"], opCall.author);
      }),

    new ServerOperation('addAll',
      operation: (ServerOperationCall opCall) {
        return opCall.colls[0].addAll(opCall.args["data"], opCall.author);
      }),

    new ServerOperation('removeAll',
      operation: (ServerOperationCall opCall) {
        return opCall.colls[0].removeAll({'_id': {'\$in': opCall.args['ids']}}, opCall.author);
      }),

];