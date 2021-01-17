// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.postgres;

import java.util.List;

import io.vlingo.actors.World;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.postgres.PostgresConfigurationProvider;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.journal.Journal;
import io.vlingo.symbio.store.journal.jdbc.JDBCJournalActor;
import io.vlingo.symbio.store.journal.jdbc.JDBCJournalActorTest;
import io.vlingo.symbio.store.journal.jdbc.JDBCJournalBatchWriter;

public class PostgresBatchJournalActorTest extends JDBCJournalActorTest {
	@Override
	protected Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
		return PostgresConfigurationProvider.testConfiguration(format);
	}

	@Override
  @SuppressWarnings("unchecked")
	protected Journal<String> journalFrom(World world, Configuration configuration, List<Dispatcher<Dispatchable<Entry<String>, State.TextState>>> dispatchers,
										  DispatcherControl dispatcherControl) throws Exception {
		JDBCJournalBatchWriter journalWriter = new JDBCJournalBatchWriter(configuration, dispatchers, dispatcherControl, 100);
		return world.stage().actorFor(Journal.class, JDBCJournalActor.class, configuration, journalWriter, 50);
	}
}
