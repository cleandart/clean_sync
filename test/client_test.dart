// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.

library client_test;

import "package:unittest/unittest.dart";
import "package:unittest/mock.dart";
import 'package:clean_sync/client.dart';
import 'package:clean_ajax/client.dart';
import 'dart:async';

class ConnectionMock extends Mock implements Connection {
  ConnectionMock(){
    this.when(callsTo('sendRequest'))
      .alwaysReturn(new Future.value({'id_prefix': 'prefix'}));
  }
}

class IdGeneratorMock extends Mock implements IdGenerator {}

void main() {
  group("Subscriber", () {
    Subscriber subscriber;
    ConnectionMock connection;
    IdGeneratorMock subscriptionIdGenerator, dataIdGenerator;
    Mock subscriptionFactory;

    setUp(() {
      connection = new ConnectionMock();
      subscriptionIdGenerator = new IdGeneratorMock();
      dataIdGenerator = new IdGeneratorMock();
      subscriptionFactory = new Mock();
    });

    test('id negotiation.', () {
      // given
      subscriber = new Subscriber.config(connection, subscriptionIdGenerator,
          dataIdGenerator, null);

      // when
      var future = subscriber.init();

      // then
      var sendRequestCalls = connection.getLogs(callsTo('sendRequest'));
      sendRequestCalls.verify(happenedOnce);
      ClientRequest request = sendRequestCalls.first.args.first();
      expect(request.args, equals({"action": "get_id_prefix"}));

      return future.then((_) {
        subscriptionIdGenerator
            .getLogs(callsTo('set prefix', 'prefix')).verify(happenedOnce);
        dataIdGenerator
            .getLogs(callsTo('set prefix', 'prefix')).verify(happenedOnce);
      });
    });

    test('subscribe when _id_prefix was not obtained.', () {
      // given
      subscriber = new Subscriber.config(connection, subscriptionIdGenerator,
          dataIdGenerator, subscriptionFactory);

      // then
      expect(()=> subscriber.subscribe("months"),
          throwsA(new isInstanceOf<MissingIdPrefixException>
            ("MissingIdPrefixException")));
    });

    test('subscribe when _id_prefix was obtained.', () {
      // given
      Map args = {'key1': 'val1'};
      subscriptionIdGenerator.when(callsTo('next')).alwaysReturn('prefix-1');

      subscriber = new Subscriber.config(connection, dataIdGenerator,
          subscriptionIdGenerator, subscriptionFactory);

      // when
      var future = subscriber.init().then((_) {
        subscriber.subscribe("months", args);
      });

      return future.then((_) {
        expect(subscriptionFactory.getLogs().first.args,
            equals(['months', connection, 'prefix-1', dataIdGenerator, args]));
      });
    });
  });
}
