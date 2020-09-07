// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc;

import io.vlingo.common.Outcome;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;

import java.util.List;
import java.util.function.Consumer;

public interface JDBCEntriesWriter {
	void appendEntries(String storeName, List<Entry<?>> entries, State.TextState rawState, Consumer<Outcome<StorageException, Result>> postAppendAction);

	void stop();
}
