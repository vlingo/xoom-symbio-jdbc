// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Definition;
import io.vlingo.common.Failure;
import io.vlingo.common.Outcome;
import io.vlingo.common.Success;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.dispatch.control.DispatcherControlActor;
import io.vlingo.symbio.store.state.StateStore;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class JDBCEntriesWriterActor extends Actor implements JDBCEntriesWriter {
	private final JDBCStorageDelegate<State.TextState> delegate;
	private final List<Dispatcher<Dispatchable<Entry<?>, State<String>>>> dispatchers;
	private final DispatcherControl dispatcherControl;

	public JDBCEntriesWriterActor(JDBCStorageDelegate<State.TextState> delegate) {
		this(delegate, null, StateStore.DefaultCheckConfirmationExpirationInterval, StateStore.DefaultConfirmationExpiration);
	}

	public JDBCEntriesWriterActor(JDBCStorageDelegate<State.TextState> delegate, List<Dispatcher<Dispatchable<Entry<?>, State<String>>>> dispatchers) {
		this(delegate, dispatchers, StateStore.DefaultCheckConfirmationExpirationInterval, StateStore.DefaultConfirmationExpiration);
	}

	public JDBCEntriesWriterActor(JDBCStorageDelegate<State.TextState> delegate, List<Dispatcher<Dispatchable<Entry<?>, State<String>>>> dispatchers,
								  long checkConfirmationExpirationInterval, long confirmationExpiration) {
		this.delegate = delegate;

		if (dispatchers != null) {
			this.dispatchers = dispatchers;
			this.dispatcherControl = stage().actorFor(
					DispatcherControl.class,
					Definition.has(
							DispatcherControlActor.class,
							new DispatcherControl.DispatcherControlInstantiator(dispatchers, delegate,
									checkConfirmationExpirationInterval, confirmationExpiration)));
		} else {
			this.dispatchers = null;
			this.dispatcherControl = null;
		}
	}

	@Override
	public void appendEntries(String storeName, List<Entry<?>> entries, State.TextState rawState, Consumer<Outcome<StorageException, Result>> postAppendAction) {
		try {
			appendEntries(entries);

			delegate.beginWrite();
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
			logger().error(getClass().getSimpleName() + " writeText() error because: " + e.getMessage(), e);
			delegate.fail();
			postAppendAction.accept(Failure.of(new StorageException(Result.Error, e.getMessage(), e)));
		}
	}

	@Override
	public void stop() {
		delegate.close();
		if (dispatcherControl != null) {
			dispatcherControl.stop();
		}
	}

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
					final String message = "vlingo-symbio-jdbc: Could not retrieve entry id.";
					logger().error(message);
					throw new IllegalStateException(message);
				}
			}
		} catch (Exception e) {
			final String message = "vlingo-symbio-jdbc: Failed to append entry because: " + e.getMessage();
			logger().error(message, e);
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
