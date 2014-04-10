import 'package:clean_ajax/client.dart';

class Transactor {
  Connection _connection;

  Transactor(this._connection);

  operation(String name, Map args) {
    _connection.send(() => new ClientRequest('sync', {'name':'jsonApply', 'args':args}));
  }
}