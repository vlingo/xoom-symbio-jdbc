// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.postgres;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration.TestConfiguration;
import io.vlingo.symbio.store.common.jdbc.postgres.PostgresConfigurationProvider;
import io.vlingo.symbio.store.state.StateStore.StorageDelegate;
import io.vlingo.symbio.store.state.jdbc.JDBCTextStateStoreActorTest;

public class PostgresJDBCTextStateStoreActorTest extends JDBCTextStateStoreActorTest {

  @Override
  protected StorageDelegate delegate() throws Exception {
    System.out.println("Starting: PostgresJDBCTextStateStoreActorTest: delegate()");
    return new PostgresStorageDelegate(configuration, world.defaultLogger());
  }

  @Override
  protected TestConfiguration testConfiguration(DataFormat format) throws Exception {
    System.out.println("Starting: PostgresJDBCTextStateStoreActorTest: testConfiguration()");
    return PostgresConfigurationProvider.testConfiguration(DataFormat.Text);
  }
}
