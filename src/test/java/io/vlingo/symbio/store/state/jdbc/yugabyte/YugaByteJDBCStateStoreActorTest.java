// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.yugabyte;

import io.vlingo.symbio.store.state.jdbc.JDBCStorageDelegate;
import org.junit.Ignore;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.yugabyte.YugaByteConfigurationProvider;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.jdbc.JDBCStateStoreActorTest;

@Ignore
public class YugaByteJDBCStateStoreActorTest extends JDBCStateStoreActorTest {
    @Override
    protected JDBCStorageDelegate<Object> delegate() throws Exception {
        System.out.println("Starting: YugaByteJDBCTextStateStoreActorTest: delegate()");
        return new YugaByteStorageDelegate(configuration, world.defaultLogger());
    }

    @Override
    protected Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
        System.out.println("Starting: YugaByteJDBCTextStateStoreActorTest: testConfiguration()");
        return YugaByteConfigurationProvider.testConfiguration(DataFormat.Text);
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
