// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
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
import io.vlingo.symbio.store.state.jdbc.JDBCStateStoreActorTest;
import io.vlingo.symbio.store.state.jdbc.JDBCStorageDelegate;

public class PostgresJDBCStateStoreActorTest extends JDBCStateStoreActorTest {

    @Override
    protected JDBCStorageDelegate<Object> delegate() throws Exception {
        System.out.println("Starting: PostgresJDBCTextStateStoreActorTest: delegate()");
        return new PostgresStorageDelegate(configuration, world.defaultLogger());
    }

    @Override
    protected TestConfiguration testConfiguration(DataFormat format) throws Exception {
        System.out.println("Starting: PostgresJDBCTextStateStoreActorTest: testConfiguration()");
        return PostgresConfigurationProvider.testConfiguration(DataFormat.Text);
    }

    @Override
    protected String someOfTypeStreams(final Class<?> type) {
      return "select * from " + tableName(type) + " where cast(s_id as integer) >= 21 and cast(s_id as integer) <= 25";
    }

    @Override
    protected String someOfTypeStreamsWithParameters(final Class<?> type) {
      return "select * from " + tableName(type) + " where cast(s_id as integer) >= ? and cast(s_id as integer) <= ?";
    }
}
