// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.jdbc;

import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.common.Outcome;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;

import java.util.List;
import java.util.function.Consumer;

public interface JDBCEntriesWriter {
	void appendEntries(String storeName, List<Entry<?>> entries, State.TextState rawState, Consumer<Outcome<StorageException, Result>> postAppendAction);

	void flush();

	void stop();

	void setLogger(Logger logger);
}
