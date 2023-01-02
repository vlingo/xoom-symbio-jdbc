// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc.yugabyte;

import java.util.List;

import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.journal.Journal;
import io.vlingo.xoom.symbio.store.journal.jdbc.*;
import io.vlingo.xoom.symbio.store.testcontainers.SharedYugaByteDbContainer;
import org.junit.Ignore;

@Ignore
public class YugaByteBatchJournalActorTest extends JDBCJournalActorTest {

	@Override
	protected Configuration.TestConfiguration testConfiguration(DataFormat format) {
		try {
			SharedYugaByteDbContainer dbContainer = SharedYugaByteDbContainer.getInstance();
			return dbContainer.testConfiguration(format);
		} catch (Exception e) {
			throw new RuntimeException("Failed to create YugaByte test configuration because: " + e.getMessage(), e);
		}
	}

	@Override
  @SuppressWarnings("unchecked")
	protected Journal<String> journalFrom(World world, Configuration configuration, List<Dispatcher<Dispatchable<Entry<String>, State.TextState>>> dispatchers,
										  DispatcherControl dispatcherControl) throws Exception {
		JDBCJournalBatchWriter journalWriter = new JDBCJournalBatchWriter(configuration, dispatchers, dispatcherControl, 100);
		return world.stage().actorFor(Journal.class, JDBCJournalActor.class, configuration, journalWriter, 50);
	}
}
