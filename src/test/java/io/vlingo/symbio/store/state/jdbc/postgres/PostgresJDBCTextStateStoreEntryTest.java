// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.postgres;

import io.vlingo.actors.Logger;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.postgres.PostgresConfigurationProvider;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.jdbc.JDBCTextStateStoreEntryTest;

public class PostgresJDBCTextStateStoreEntryTest extends JDBCTextStateStoreEntryTest {

    @Override
    protected StateStore.StorageDelegate storageDelegate(Configuration.TestConfiguration configuration, Logger logger) {
        return new PostgresStorageDelegate(configuration, logger);
    }

    @Override
    protected Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
        System.out.println("Starting: PostgresJDBCTextStateStoreEntryActorTest: testConfiguration()");
        return PostgresConfigurationProvider.testConfiguration(format);
    }
}
