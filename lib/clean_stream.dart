library clean_sync.clean_stream;

import 'dart:io';
import 'dart:async';
import 'package:logging/logging.dart';
import 'dart:convert';

Logger _logger = new Logger("clean_socket");

class Tuple {
  var fst;
  var snd;
  Tuple(this.fst, this.snd);
}

Tuple decodeLeadingNum(String message) {
  // Take while it's a digit
  List codeUnits = message.codeUnits.takeWhile((c) => ((c >= 48) && (c <= 57))).toList();
  // If there are only digits, the leading number is problably not transfered whole
  if ((codeUnits.length == message.length) || (codeUnits.isEmpty)) return new Tuple(-1, -1);
  return new Tuple(num.parse(new String.fromCharCodes(codeUnits)), codeUnits.length);
}

/**
 * Takes a [message] of potentially concatenated JSONs
 * and returns List of separate JSONs. If the message is incomplete,
 * the incomplete part is stored in [incompleteJson]
 * */
List<String> getJSONs(String message, [Map incompleteJson]) {
  List<String> jsons = [];
  int messageLength = 0;
  int lastAdditionAt = 0;
  _logger.finest("Messages: $message");
  _logger.finest("From previous iteration: $incompleteJson");
  if (incompleteJson == null) incompleteJson = {};
  if (incompleteJson.containsKey("msg")) {
    // Previous JSON was not sent entirely
    message = incompleteJson["msg"] + message;
    _logger.finest("New message: $message");
  }

  int i = 0;
  while (i < message.length) {
    // Beginning of new message
    // Performance upgrade, there's not going to be JSON longer than 10 bil chars..
    // Returns -1 if there are only digits or no digits
    // Assert = message[i] is a beginning of some valid message => the leading
    // few characters determine the length of message
    Tuple messageInfo = decodeLeadingNum(message.substring(i, i+10));
    messageLength = messageInfo.fst;
    if (messageLength == -1) {
      // Length of string was not sent entirely
      break;
    }
    i += messageInfo.snd;
    if (messageLength+i > message.length) {
      // We want to send more chars than this message contains =>
      // it was not sent entirely
      break;
    }
    jsons.add(message.substring(i, i+messageLength));
    lastAdditionAt = i+messageLength;
    i += messageLength;
  }
  if (lastAdditionAt != message.length-1) {
    // message is incomplete
    incompleteJson["msg"] = message.substring(lastAdditionAt);
  } else incompleteJson["msg"] = "";
  _logger.fine("Jsons: $jsons");
  return jsons;
}

writeJSON(IOSink iosink, String json) =>
    iosink.write("${json.length}${json}");

Stream toJsonStream(Stream stream) =>
    stream.transform(new StreamTransformer(
        (Stream input, bool cancelOnError) {
          StreamController sc = new StreamController.broadcast();
          Map incompleteJson = {};
          input.listen((List<int> data) {
              Iterable jsons = getJSONs(UTF8.decode(data), incompleteJson).map(JSON.decode);
              jsons.forEach((json) => sc.add(json));
            },
            onError: sc.addError,
            onDone: sc.close,
            cancelOnError: cancelOnError
          );

          return sc.stream.listen(null);
        })
    );