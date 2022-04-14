// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.jdbc.postgres;

import java.util.List;

import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration.TestConfiguration;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.state.StateStore;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCEntriesInstantWriter;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCEntriesWriter;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCStateStoreActor;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCStateStoreActorTest;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCStorageDelegate;
import io.vlingo.xoom.symbio.store.testcontainers.SharedPostgreSQLContainer;

public class PostgresJDBCStateStoreActorTest extends JDBCStateStoreActorTest {
    private SharedPostgreSQLContainer postgresContainer = SharedPostgreSQLContainer.getInstance();

    @Override
    protected JDBCStorageDelegate<Object> delegate() throws Exception {
        System.out.println("Starting: PostgresJDBCTextStateStoreActorTest: delegate()");
        return new PostgresStorageDelegate(configuration, world.defaultLogger());
    }

    @Override
    protected TestConfiguration testConfiguration(DataFormat format) throws Exception {
        System.out.println("Starting: PostgresJDBCTextStateStoreActorTest: testConfiguration()");
        return postgresContainer.testConfiguration(format);
    }

    @Override
    protected String someOfTypeStreams(final Class<?> type) {
      return "select * from " + tableName(type) + " where cast(s_id as integer) >= 21 and cast(s_id as integer) <= 25";
    }

    @Override
    protected String someOfTypeStreamsWithParameters(final Class<?> type) {
      return "select * from " + tableName(type) + " where cast(s_id as integer) >= ? and cast(s_id as integer) <= ?";
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
