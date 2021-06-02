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

  private final Configuration configuration;

  public HSQLDBJdbiEntryReaderTest() {
    try {
      this.configuration = HSQLDBConfigurationProvider.testConfiguration(DataFormat.Text);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create test configuration because: " + e.getMessage(), e);
    }
  }

  @Override
  protected JdbiOnDatabase jdbiOnDatabase() throws Exception {
    final JdbiOnDatabase jdbi = JdbiOnHSQLDB.openUsing(configuration);
    jdbi.handle().execute("DROP SCHEMA PUBLIC CASCADE");
    return jdbi;
  }

  @Override
  protected Configuration configuration() {
    return configuration;
  }
}
