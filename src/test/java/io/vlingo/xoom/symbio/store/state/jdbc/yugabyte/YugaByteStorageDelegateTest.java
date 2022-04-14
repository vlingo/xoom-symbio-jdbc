// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.jdbc.yugabyte;

import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCStorageDelegate;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCStorageDelegateTest;
import io.vlingo.xoom.symbio.store.testcontainers.SharedYugaByteDbContainer;
import org.junit.Ignore;

@Ignore
public class YugaByteStorageDelegateTest extends JDBCStorageDelegateTest {
    private SharedYugaByteDbContainer dbContainer = SharedYugaByteDbContainer.getInstance();

    @Override
    protected JDBCStorageDelegate<Object> storageDelegate(Configuration.TestConfiguration configuration, Logger logger) {
        return new YugaByteStorageDelegate(configuration, logger);
    }

    @Override
    protected Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
        return dbContainer.testConfiguration(format);
    }
}
