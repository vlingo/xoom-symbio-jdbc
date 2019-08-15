// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.hsqldb.HSQLDBConfigurationProvider;

public class HSQLDBJdbiEntryReaderTest extends JdbiObjectStoreEntryReaderTest {

  @Override
  protected JdbiOnDatabase jdbiOnDatabase() throws Exception {
    final JdbiOnDatabase jdbi = JdbiOnHSQLDB.openUsing(HSQLDBConfigurationProvider.testConfiguration(DataFormat.Text));
    jdbi.handle().execute("DROP SCHEMA PUBLIC CASCADE");
    return jdbi;
  }
}
