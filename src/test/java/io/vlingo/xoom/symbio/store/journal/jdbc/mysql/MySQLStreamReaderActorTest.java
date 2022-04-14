// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc.mysql;

import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.journal.jdbc.JDBCStreamReaderActorTest;
import io.vlingo.xoom.symbio.store.testcontainers.SharedMySQLContainer;

public class MySQLStreamReaderActorTest extends JDBCStreamReaderActorTest {

  @Override
  protected Configuration.TestConfiguration testConfiguration(DataFormat format) {
    try {
      SharedMySQLContainer mysqlContainer = SharedMySQLContainer.getInstance();
      return mysqlContainer.testConfiguration(format);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create MySQL test configuration because: " + e.getMessage(), e);
    }
  }
}
