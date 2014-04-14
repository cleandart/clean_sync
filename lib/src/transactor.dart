import 'package:clean_ajax/client.dart';
import 'dart:async';

class Transactor {
  Connection _connection;

  Transactor(this._connection);

  Future operation(String name, Map args) {
    return _connection.send(() => new ClientRequest('sync', {'action':name, 'args':args}));
  }
}