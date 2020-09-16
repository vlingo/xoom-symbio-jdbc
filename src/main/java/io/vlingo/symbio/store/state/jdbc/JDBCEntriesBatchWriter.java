// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc;

import io.vlingo.actors.Logger;
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class JDBCEntriesBatchWriter implements JDBCEntriesWriter {
	private final JDBCStorageDelegate<State.TextState> delegate;
	private final List<Dispatcher<Dispatchable<? extends Entry<?>, ? extends State<?>>>> dispatchers;
	private final DispatcherControl dispatcherControl;
	private final BatchEntries batchEntries;
	private Logger logger;

	public JDBCEntriesBatchWriter(JDBCStorageDelegate<State.TextState> delegate, int maxBatchEntries) {
		this(delegate, null, null, maxBatchEntries);
	}

	public JDBCEntriesBatchWriter(JDBCStorageDelegate<State.TextState> delegate,
								  List<Dispatcher<Dispatchable<? extends Entry<?>, ? extends State<?>>>> dispatchers,
								  DispatcherControl dispatcherControl,
								  int maxBatchEntries) {
		this.delegate = delegate;
		this.dispatchers = dispatchers;
		this.dispatcherControl = dispatcherControl;
		this.batchEntries = new BatchEntries(maxBatchEntries);
	}

	@Override
	public void appendEntries(String storeName, List<Entry<?>> entries, State.TextState rawState, Consumer<Outcome<StorageException, Result>> postAppendAction) {
		batchEntries.add(new BatchEntry(storeName, entries, rawState, postAppendAction));
		if (batchEntries.capacityExceeded()) {
			flush();
		}
	}

	@Override
	public void flush() {
		appendBatchedEntries();

		if (batchEntries.size() > 0) {
			try {
				delegate.beginWrite();

				Map<String, List<State.TextState>> states = batchEntries.states();
				for (Map.Entry<String, List<State.TextState>> storeStates : states.entrySet()) {
					final PreparedStatement writeStatesStatement = delegate.writeExpressionFor(storeStates.getKey(), storeStates.getValue());
					writeStatesStatement.executeBatch();
					writeStatesStatement.clearBatch();
				}

				List<Dispatchable<Entry<?>, State<String>>> dispatchables = batchEntries.collectDispatchables();
				final PreparedStatement writeDispatchablesStatement = delegate.dispatchableWriteExpressionFor(dispatchables);
				writeDispatchablesStatement.executeBatch();
				writeDispatchablesStatement.clearBatch();

				delegate.complete();
				batchEntries.completedWith(Success.of(Result.Success));
				batchEntries.clear();
			} catch (Exception e) {
				logger.error(getClass().getSimpleName() + " appendEntries() failed because: " + e.getMessage(), e);
				batchEntries.completedWith(Failure.of(new StorageException(Result.Error, e.getMessage(), e)));
				delegate.fail();
			}
		}
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

	private void appendBatchedEntries() {
		List<Entry<?>> allEntries = batchEntries.collectEntries();
		if (allEntries.size() > 0) {
			try {
				PreparedStatement appendStatement = delegate.appendExpressionFor(allEntries);
				final int[] countList = appendStatement.executeBatch();
				if (Arrays.stream(countList).anyMatch(id -> id == -1L)) {
					final String message = "vlingo-symbio-jdbc: Failed append entries.";
					logger.error(message);
					throw new IllegalStateException(message);
				}

				ResultSet resultSet = appendStatement.getGeneratedKeys();
				for (int i = 0; resultSet.next(); i++) {
					long id = resultSet.getLong(1);
					((BaseEntry) allEntries.get(i)).__internal__setId(Long.toString(id));
				}

				appendStatement.clearBatch();
			} catch (Exception e) {
				final String message = "vlingo-symbio-jdbc: Failed to append entries because: " + e.getMessage();
				logger.error(message, e);
				throw new IllegalStateException(message, e);
			}
		}
	}

	static class BatchEntries {
		private final List<BatchEntry> entries;
		private final int maxCapacity;

		BatchEntries(int maxCapacity) {
			this.entries = new ArrayList<>(maxCapacity);
			this.maxCapacity = maxCapacity;

			if (maxCapacity <= 0) {
				throw new IllegalArgumentException("Illegal capacity: " + maxCapacity);
			}
		}

		void add(BatchEntry entry) {
			entries.add(entry);
		}

		boolean capacityExceeded() {
			return entries.size() >= maxCapacity;
		}

		List<Dispatchable<Entry<?>, State<String>>> collectDispatchables() {
			return entries.stream()
					.map(batch -> batch.getDispatchable())
					.collect(Collectors.toList());
		}

		List<Entry<?>> collectEntries() {
			return entries.stream()
					.map(batch -> batch.entries)
					.reduce(new ArrayList<Entry<?>>(), (collected, current) -> {
						collected.addAll(current);
						return collected;
					});
		}

		Map<String, List<State.TextState>> states() {
			return entries.stream()
					.collect(Collectors.groupingBy((BatchEntry batch) -> batch.storeName,
							Collectors.mapping(batch -> batch.rawState, Collectors.toList())));
		}

		void completedWith(Outcome<StorageException, Result> of) {
			entries.stream()
					.forEach(batch -> batch.postAppendAction.accept(of));
		}

		void clear() {
			entries.clear();
		}

		int size() {
			return entries.size();
		}
	}

	static class BatchEntry {
		final String storeName;
		final List<Entry<?>> entries;
		final State.TextState rawState;
		final Consumer<Outcome<StorageException, Result>> postAppendAction;

		private Dispatchable<Entry<?>, State<String>> dispatchable = null;
		boolean failed = false;

		BatchEntry(String storeName, List<Entry<?>> entries, State.TextState rawState, Consumer<Outcome<StorageException, Result>> postAppendAction) {
			this.storeName = storeName;
			this.entries = entries;
			this.rawState = rawState;
			this.postAppendAction = postAppendAction;
		}

		public Dispatchable<Entry<?>, State<String>> getDispatchable() {
			if (dispatchable == null) {
				final String dispatchId = storeName + ":" + rawState.id;
				dispatchable = new Dispatchable<>(dispatchId, LocalDateTime.now(), rawState.asTextState(), entries);
			}

			return dispatchable;
		}
	}
}
