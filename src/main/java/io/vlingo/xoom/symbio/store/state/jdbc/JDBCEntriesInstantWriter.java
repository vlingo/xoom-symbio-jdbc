// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.List;
import java.util.function.Consumer;

import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.common.Failure;
import io.vlingo.xoom.common.Outcome;
import io.vlingo.xoom.common.Success;
import io.vlingo.xoom.symbio.BaseEntry;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;

public class JDBCEntriesInstantWriter implements JDBCEntriesWriter {
	private final JDBCStorageDelegate<State.TextState> delegate;
	private final List<Dispatcher<Dispatchable<? extends Entry<?>, ? extends State<?>>>> dispatchers;
	private final DispatcherControl dispatcherControl;
	private Logger logger;

	public JDBCEntriesInstantWriter(JDBCStorageDelegate<State.TextState> delegate) {
		this(delegate, null, null);
	}

	public JDBCEntriesInstantWriter(JDBCStorageDelegate<State.TextState> delegate, List<Dispatcher<Dispatchable<? extends Entry<?>, ? extends State<?>>>> dispatchers,
								  DispatcherControl dispatcherControl) {
		this.delegate = delegate;
		this.dispatchers = dispatchers;
		this.dispatcherControl = dispatcherControl;
	}

	@Override
	public void appendEntries(String storeName, List<Entry<?>> entries, State.TextState rawState, Consumer<Outcome<StorageException, Result>> postAppendAction) {
		try {
			delegate.beginWrite();
			appendEntries(entries);

			final PreparedStatement writeStatement = delegate.writeExpressionFor(storeName, rawState);
			writeStatement.execute();
			final String dispatchId = storeName + ":" + rawState.id;

			final Dispatchable<Entry<?>, State<String>> dispatchable = buildDispatchable(dispatchId, rawState, entries);
			final PreparedStatement dispatchableStatement = delegate.dispatchableWriteExpressionFor(dispatchable);
			dispatchableStatement.execute();
			delegate.complete();

			dispatch(dispatchable);
			postAppendAction.accept(Success.of(Result.Success));
		} catch (Exception e) {
			logger.error(getClass().getSimpleName() + " appendEntries() error because: " + e.getMessage(), e);
			delegate.fail();
			postAppendAction.accept(Failure.of(new StorageException(Result.Error, e.getMessage(), e)));
		}
	}

	@Override
	public void flush() {
		// No flush; this is an instant writer
	}

	@Override
	public void stop() {
		if (dispatcherControl != null) {
			dispatcherControl.stop();
		}
	}

	@Override
	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	@SuppressWarnings("rawtypes")
  private void appendEntries(List<Entry<?>> entries) {
		try {
			for (final Entry<?> entry : entries) {
				long id = -1L;
				final PreparedStatement appendStatement = delegate.appendExpressionFor(entry);
				final int count = appendStatement.executeUpdate();
				if (count == 1) {
					final PreparedStatement queryLastIdentityStatement = delegate.appendIdentityExpression();
					try (final ResultSet result = queryLastIdentityStatement.executeQuery()) {
						if (result.next()) {
							id = result.getLong(1);
							((BaseEntry) entry).__internal__setId(Long.toString(id));
						}
					}
				}
				if (id == -1L) {
					final String message = "xoom-symbio-jdbc: Could not retrieve entry id.";
					logger.error(message);
					throw new IllegalStateException(message);
				}
			}
		} catch (Exception e) {
			final String message = "xoom-symbio-jdbc: Failed to append entry because: " + e.getMessage();
			logger.error(message, e);
			throw new IllegalStateException(message, e);
		}
	}

	private Dispatchable<Entry<?>, State<String>> buildDispatchable(final String dispatchId, final State<String> state, final List<Entry<?>> entries) {
		return new Dispatchable<>(dispatchId, LocalDateTime.now(), state.asTextState(), entries);
	}

	private void dispatch(final Dispatchable<Entry<?>, State<String>> dispatchable) {
		if (this.dispatchers != null) {
			dispatchers.forEach(d -> d.dispatch(dispatchable));
		}
	}
}
