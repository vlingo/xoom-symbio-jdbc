// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.mysql;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.mysql.MySQLConfigurationProvider;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.jdbc.JDBCStateStoreActorTest;

public class MySQLJDBCStateStoreActorTest extends JDBCStateStoreActorTest {
	@Override
	protected StateStore.StorageDelegate delegate() throws Exception {
		System.out.println("Starting: MySQLJDBCTextStateStoreActorTest: delegate()");
		return new MySQLStorageDelegate(configuration, world.defaultLogger());
	}

	@Override
	protected Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
		System.out.println("Starting: MySQLJDBCTextStateStoreActorTest: testConfiguration()");
		return MySQLConfigurationProvider.testConfiguration(DataFormat.Text);
	}
}
