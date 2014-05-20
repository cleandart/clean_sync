library clean_sync.client_operations;

import 'package:logging/logging.dart';
import 'package:clean_sync/client.dart';
import 'package:clean_sync/operations.dart';

Logger logger = new Logger('mongo_wrapper_logger');

// First element is ServerOperation, second is equivalent ClientOperation
List operations = [

    new ClientOperation('add',
      operation: (ClientOperationCall opCall) {
        opCall.colls[0].add(opCall.args, author:opCall.author);
      }),

    new ClientOperation('remove',
      operation: (ClientOperationCall opCall){
        opCall.colls[0].remove(opCall.args["_id"], author:opCall.author);
      }),

    new ClientOperation('addAll',
      operation: (ClientOperationCall opCall){
        opCall.colls[0].addAll(opCall.args["data"], author:opCall.author);
      }),

    new ClientOperation('removeAll',
      operation: (ClientOperationCall opCall){
        opCall.colls[0].remove(opCall.args["_id"], author:opCall.author);
      }),
];
