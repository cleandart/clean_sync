// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.


import 'publisher_test.dart' as publisher_test;
import 'mongo_provider_test.dart' as mongo_provider_test;
import 'subscription_test.dart' as subscription_test;
import 'id_generator_test.dart' as id_generator_test;
import 'exception_test.dart' as exception_test;
import 'collection_modification_test.dart' as collection_modification_test;
import 'subs_random_test.dart' as subs_random_test;
import 'mongo_provider_random_test.dart' as mp_random_test;
import 'mongo_server_test.dart' as mongo_server_test;
import 'transactor_test.dart' as transactor_test;
import 'transactor_integration_test.dart' as transactor_integration_test;
import 'package:unittest/unittest.dart';
import 'package:unittest/vm_config.dart';
import 'package:logging/logging.dart';
import 'package:useful/useful.dart';
import 'package:clean_sync/server.dart';


final Logger logger = new Logger('clean_sync');


main() {
  run(new VMConfiguration());
}

run(SimpleConfiguration configuration) {
  configuration.timeout = new Duration(seconds: 47);
  unittestConfiguration = configuration;
  hierarchicalLoggingEnabled = true;
  setupDefaultLogHandler();

  mongo_provider_test.main();
  publisher_test.run();
  subscription_test.run();
  id_generator_test.main();
  exception_test.run();
  mongo_server_test.run();
  transactor_test.main();
  transactor_integration_test.main();
  collection_modification_test.run();
  subs_random_test.run(100, new DummyCache());
  subs_random_test.run(100, new Cache(new Duration(milliseconds: 100), 10000));
  mp_random_test.run(30);
}
