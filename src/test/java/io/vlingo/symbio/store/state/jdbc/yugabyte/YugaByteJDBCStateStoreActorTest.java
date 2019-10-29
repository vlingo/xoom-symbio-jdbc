// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.yugabyte;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.yugabyte.YugaByteConfigurationProvider;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.jdbc.JDBCStateStoreActorTest;
import org.junit.Ignore;

@Ignore
public class YugaByteJDBCStateStoreActorTest extends JDBCStateStoreActorTest {
    @Override
    protected StateStore.StorageDelegate delegate() throws Exception {
        System.out.println("Starting: YugaByteJDBCTextStateStoreActorTest: delegate()");
        return new YugaByteStorageDelegate(configuration, world.defaultLogger());
    }

    @Override
    protected Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
        System.out.println("Starting: YugaByteJDBCTextStateStoreActorTest: testConfiguration()");
        return YugaByteConfigurationProvider.testConfiguration(DataFormat.Text);
    }
}
