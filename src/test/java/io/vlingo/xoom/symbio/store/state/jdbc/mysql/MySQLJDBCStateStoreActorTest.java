// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.jdbc.mysql;

import java.util.List;

import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.state.StateStore;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCEntriesInstantWriter;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCEntriesWriter;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCStateStoreActor;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCStateStoreActorTest;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCStorageDelegate;
import io.vlingo.xoom.symbio.store.testcontainers.SharedMySQLContainer;

public class MySQLJDBCStateStoreActorTest extends JDBCStateStoreActorTest {
    private SharedMySQLContainer mysqlContainer = SharedMySQLContainer.getInstance();

    @Override
    protected JDBCStorageDelegate<Object> delegate() throws Exception {
        System.out.println("Starting: MySQLJDBCTextStateStoreActorTest: delegate()");
        return new MySQLStorageDelegate(configuration, world.defaultLogger());
    }

    @Override
    protected Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
        System.out.println("Starting: MySQLJDBCTextStateStoreActorTest: testConfiguration()");
        return mysqlContainer.testConfiguration(format);
    }

    @Override
    protected String someOfTypeStreams(final Class<?> type) {
      return "select * from " + tableName(type) + " where s_id between 21 and 25";
    }

    @Override
    protected String someOfTypeStreamsWithParameters(final Class<?> type) {
      return "select * from " + tableName(type) + " where s_id between ? and ?";
    }

    @Override
    protected StateStore stateStoreFrom(World world,
                                        JDBCStorageDelegate<State.TextState> delegate,
                                        List<Dispatcher<Dispatchable<? extends Entry<?>, ? extends State<?>>>> dispatchers,
                                        DispatcherControl dispatcherControl) {
        JDBCEntriesWriter entriesWriter = new JDBCEntriesInstantWriter(delegate, dispatchers, dispatcherControl);
        return world.actorFor(StateStore.class, JDBCStateStoreActor.class, delegate, entriesWriter);
    }
}
