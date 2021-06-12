// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc.postgres;

import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.journal.jdbc.JDBCJournalReaderActorTest;
import io.vlingo.xoom.symbio.store.testcontainers.SharedPostgreSQLContainer;

public class PostgresJournalReaderActorTest extends JDBCJournalReaderActorTest {

  @Override
  protected Configuration.TestConfiguration testConfiguration(DataFormat format) {
    try {
      SharedPostgreSQLContainer postgresContainer = SharedPostgreSQLContainer.getInstance();
      return postgresContainer.testConfiguration(format);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create PostgreSQL test configuration because: " + e.getMessage(), e);
    }
  }
}
