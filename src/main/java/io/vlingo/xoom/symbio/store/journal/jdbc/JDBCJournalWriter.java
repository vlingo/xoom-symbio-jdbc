// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc;

import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.common.Outcome;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State.TextState;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public interface JDBCJournalWriter {
	void appendEntry(final String streamName, final int streamVersion, Entry<String> entry, Optional<TextState> snapshotState,
						  Consumer<Outcome<StorageException, Result>> postAppendAction);

	void appendEntries(final String streamName, final int fromStreamVersion, final List<Entry<String>> entries, Optional<TextState> snapshotState,
						  Consumer<Outcome<StorageException, Result>> postAppendAction);

	void flush();

	void stop();

	void setLogger(Logger logger);
}
