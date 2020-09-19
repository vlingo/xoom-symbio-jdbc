// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc;

import io.vlingo.actors.Logger;
import io.vlingo.common.Outcome;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;

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
