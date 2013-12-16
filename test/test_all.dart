// Copyright (c) 2013, the Clean project authors.  Please see the AUTHORS file
// for details. All rights reserved. Use of this source code is governed by a
// BSD-style license that can be found in the LICENSE file.


import 'publisher_test.dart' as publisher_test;
import 'mongo_provider_test.dart' as mongo_provider_test;
import 'client_test.dart' as client_test;
import 'id_generator_test.dart' as id_generator_test;

import 'package:unittest/unittest.dart';
import 'package:unittest/vm_config.dart';

main() {
  run(new VMConfiguration());
}

run(configuration) {
  unittestConfiguration = configuration;

  mongo_provider_test.main();
  publisher_test.main();
  client_test.main();
  id_generator_test.main();
}
