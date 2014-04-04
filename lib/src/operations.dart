import 'dart:async';

class ServerOperation {
  Function before;
  Function after;
  Function operation;
  List collections;
  String name;

  ServerOperation(this.name, this.operation, this.before, this.after, this.collections);
}

class ClientOperation {
  String name;
  Function operation;
}

class ServerOperations {
  Map operations;

  ServerOperations() {
    operations = new Map();
  }

  register(name, operation, before, after, collections){
    operations[name] = new ServerOperation(name, operation, before, after, collections);
  }

  performServer(String name, List docs, Map args, String authenticatedUserId) {
    ServerOperation op = operations[name];
    List fullDocs;
    int i;
    Map user;
    Future.forEach(docs, (doc){
      i++;
      return db.collection(op.collections[i]).find({'_id': docs[i]}).one()
          .then((fullDoc) => docs.add(fullDoc));

    })
    .then((_){
      return db.collection(userColName).find({'_id': authenticatedUserId}).one();
    })
    .then((_user){
      user = _user;
      return op.before(fullDocs, args, user);
    })
    .then((_) {
      return op.operation(docs, args);
    }).then((_) {
      return op.after(docs, args, user);
    });
  }

  performClient(String name, List docs, Map args){
    ServerOperation op = operations[name];
    op.operation(docs, args);
    connection.send('sync', {'name': name, 'docs': docs, 'args': args});
  }
}
