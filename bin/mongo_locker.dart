import 'dart:async';
import 'package:clean_sync/locker.dart';

main(List<String> args) {
  if (args.length != 2) {
    print("You have to specify url and port");
    return new Future.value(null);
  }
  var url = args[0];
  var port = num.parse(args[1]);
  return Locker.bind(url, port)
      .then((_) => print("Locker running on ${args}"));
}