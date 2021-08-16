// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jdbi;

import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.hsqldb.HSQLDBConfigurationProvider;

public class HSQLDBJdbiEntryReaderTest extends JdbiObjectStoreEntryReaderTest {

  @Override
  protected JdbiOnDatabase jdbiOnDatabase(Configuration configuration) throws Exception {
    final JdbiOnDatabase jdbi = JdbiOnHSQLDB.openUsing(configuration);
    jdbi.handle().execute("DROP SCHEMA PUBLIC CASCADE");
    return jdbi;
  }

  @Override
  protected Configuration.TestConfiguration configuration() {
    try {
      return HSQLDBConfigurationProvider.testConfiguration(DataFormat.Text, "testdb", 5);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test configuration because: " + e.getMessage(), e);
    }
  }
}
