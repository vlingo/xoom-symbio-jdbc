// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.mysql;

import io.vlingo.actors.World;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.mysql.MySQLConfigurationProvider;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.jdbc.*;

import java.util.Arrays;
import java.util.List;

public class MySQLJDBCStateStoreActorTest extends JDBCStateStoreActorTest {
    @Override
    protected JDBCStorageDelegate<Object> delegate() throws Exception {
        System.out.println("Starting: MySQLJDBCTextStateStoreActorTest: delegate()");
        return new MySQLStorageDelegate(configuration, world.defaultLogger());
    }

    @Override
    protected Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
        System.out.println("Starting: MySQLJDBCTextStateStoreActorTest: testConfiguration()");
        return MySQLConfigurationProvider.testConfiguration(DataFormat.Text);
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
