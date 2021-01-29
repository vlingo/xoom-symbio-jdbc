// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.postgres;

import java.util.List;

import io.vlingo.actors.World;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.jdbc.JDBCEntriesBatchWriter;
import io.vlingo.symbio.store.state.jdbc.JDBCEntriesWriter;
import io.vlingo.symbio.store.state.jdbc.JDBCStateStoreActor;
import io.vlingo.symbio.store.state.jdbc.JDBCStorageDelegate;

public class PostgresJDBCBatchStateStoreActorTest extends PostgresJDBCStateStoreActorTest {
	@Override
	protected StateStore stateStoreFrom(World world,
										JDBCStorageDelegate<State.TextState> delegate,
										List<Dispatcher<Dispatchable<? extends Entry<?>, ? extends State<?>>>> dispatchers,
										DispatcherControl dispatcherControl) {
		JDBCEntriesWriter entriesWriter = new JDBCEntriesBatchWriter(delegate, dispatchers, dispatcherControl, 50);
		return world.actorFor(StateStore.class, JDBCStateStoreActor.class, delegate, entriesWriter, 100, null);
	}
}
