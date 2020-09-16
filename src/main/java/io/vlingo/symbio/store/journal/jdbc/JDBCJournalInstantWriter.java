// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc;

import com.google.gson.Gson;
import io.vlingo.actors.Logger;
import io.vlingo.common.Failure;
import io.vlingo.common.Outcome;
import io.vlingo.common.Success;
import io.vlingo.common.Tuple2;
import io.vlingo.common.identity.IdentityGenerator;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class JDBCJournalInstantWriter implements JDBCJournalWriter {
	private final Configuration configuration;
	private final Connection connection;
	private final JDBCQueries queries;
	private final List<Dispatcher<Dispatchable<Entry<String>, TextState>>> dispatchers;
	private final DispatcherControl dispatcherControl;
	private final Gson gson;
	private final IdentityGenerator dispatchablesIdentityGenerator;

	private Logger logger;

	public JDBCJournalInstantWriter(Configuration configuration, List<Dispatcher<Dispatchable<Entry<String>, TextState>>> dispatchers,
									DispatcherControl dispatcherControl) throws Exception {
		this.configuration = configuration;
		this.connection = configuration.connection;
		this.dispatchers = dispatchers;
		this.dispatcherControl = dispatcherControl;
		this.gson = new Gson();
		this.dispatchablesIdentityGenerator = new IdentityGenerator.RandomIdentityGenerator();

		this.connection.setAutoCommit(false);
		this.queries = JDBCQueries.queriesFor(configuration.connection);
		this.queries.createTables();
	}

	@Override
	public void appendEntry(String streamName, int streamVersion, Entry<String> entry, Optional<TextState> snapshotState,
							Consumer<Outcome<StorageException, Result>> postAppendAction) {
		insertEntry(streamName, streamVersion, entry, postAppendAction);
		snapshotState.ifPresent(state -> insertSnapshot(streamName, streamVersion, state, postAppendAction));
		final Dispatchable<Entry<String>, TextState> dispatchable =
				insertDispatchable(streamName, streamVersion, Collections.singletonList(entry), snapshotState.orElse(null), postAppendAction);
		doCommit(postAppendAction);
		dispatch(dispatchable);
		postAppendAction.accept(Success.of(Result.Success));
	}

	@Override
	public void appendEntries(String streamName, int fromStreamVersion, List<Entry<String>> entries, Optional<TextState> snapshotState,
							  Consumer<Outcome<StorageException, Result>> postAppendAction) {
		int version = fromStreamVersion;
		for (Entry<String> entry : entries) {
			insertEntry(streamName, version++, entry, postAppendAction);
		}

		snapshotState.ifPresent(state -> insertSnapshot(streamName, fromStreamVersion, state, postAppendAction));
		final Dispatchable<Entry<String>, TextState> dispatchable =
				insertDispatchable(streamName, fromStreamVersion, entries, snapshotState.orElse(null), postAppendAction);
		doCommit(postAppendAction);
		dispatch(dispatchable);
		postAppendAction.accept(Success.of(Result.Success));
	}

	@Override
	public void stop() {
		if (dispatcherControl != null) {
			dispatcherControl.stop();
		}

		try {
			queries.close();
		} catch (SQLException e) {
			// ignore
		}
	}

	@Override
	public void setLogger(Logger logger) {
		this.logger = logger;
	}

	private String buildDispatchId(String streamName, int streamVersion) {
		return streamName + ":" + streamVersion + ":" + dispatchablesIdentityGenerator.generate().toString();
	}

	private void dispatch(final Dispatchable<Entry<String>, TextState> dispatchable) {
		if (dispatchers != null) {
			// dispatch only if insert successful
			this.dispatchers.forEach(d -> d.dispatch(dispatchable));
		}
	}

	private void doCommit(final Consumer<Outcome<StorageException, Result>> postAppendAction) {
		try {
			configuration.connection.commit();
		} catch (final SQLException e) {
			postAppendAction.accept(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)));
			logger.error("vlingo-symbio-jdbc:journal-" + configuration.databaseType + ": Could not complete transaction", e);
			throw new IllegalStateException(e);
		}
	}

	private Dispatchable<Entry<String>, TextState> insertDispatchable(String streamName, int streamVersion, final List<Entry<String>> entries,
																	  TextState snapshotState, Consumer<Outcome<StorageException, Result>> postAppendAction) {
		final String id = buildDispatchId(streamName, streamVersion);
		final Dispatchable<Entry<String>, TextState> dispatchable = new Dispatchable<>(id, LocalDateTime.now(), snapshotState, entries);
		String dbType = configuration.databaseType.toString();

		try {
			final String encodedEntries = dispatchable.hasEntries() ?
					dispatchable.entries().stream().map(Entry::id).collect(Collectors.joining(JDBCDispatcherControlDelegate.DISPATCHEABLE_ENTRIES_DELIMITER)) :
					"";

			final Tuple2<PreparedStatement, Optional<String>> insertDispatchable;

			final String dispatchableId = dispatchable.id();

			if (dispatchable.state().isPresent()) {
				final State<String> state = dispatchable.typedState();

				insertDispatchable =
						queries.prepareInsertDispatchableQuery(
								dispatchableId,
								configuration.originatorId,
								state.id,
								state.data,
								state.dataVersion,
								state.type,
								state.typeVersion,
								gson.toJson(state.metadata),
								encodedEntries);
			} else {
				insertDispatchable =
						queries.prepareInsertDispatchableQuery(
								dispatchableId,
								configuration.originatorId,
								null,
								null,
								0,
								null,
								0,
								null,
								encodedEntries);
			}

			if (insertDispatchable._1.executeUpdate() != 1) {
				logger.error("vlingo-symbio-jdbc:journal-" + dbType + ": Could not insert dispatchable with id " + dispatchable.id());
				throw new IllegalStateException("vlingo-symbio-jdbc:journal-" + dbType + ": Could not insert snapshot");
			}
		} catch (final SQLException e) {
			postAppendAction.accept(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)));
			logger.error("vlingo-symbio-jdbc:journal-" + dbType + ": Could not insert dispatchable with id " + dispatchable.id(), e);
			throw new IllegalStateException(e);
		}

		return dispatchable;
	}

	private void insertSnapshot(final String streamName, final int streamVersion, final TextState snapshotState,
								final Consumer<Outcome<StorageException, Result>> postAppendAction) {
		DatabaseType databaseType = configuration.databaseType;

		try {
			final Tuple2<PreparedStatement, Optional<String>> insertSnapshot =
					queries.prepareInsertSnapshotQuery(
							streamName,
							streamVersion,
							snapshotState.data,
							snapshotState.dataVersion,
							snapshotState.type,
							snapshotState.typeVersion,
							gson.toJson(snapshotState.metadata));

			if (insertSnapshot._1.executeUpdate() != 1) {
				logger.error("vlingo-symbio-jdbc:journal-" + databaseType + ": Could not insert snapshot with id " + snapshotState.id);
				throw new IllegalStateException("vlingo-symbio-jdbc:journal-" + databaseType + ": Could not insert snapshot");
			}
		} catch (final SQLException e) {
			postAppendAction.accept(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)));
			logger.error("vlingo-symbio-jdbc:journal-" + databaseType + ": Could not insert event with id " + snapshotState.id, e);
			throw new IllegalStateException(e);
		}
	}

	private void insertEntry(String streamName, int streamVersion, Entry<String> entry, Consumer<Outcome<StorageException, Result>> postAppendAction) {
		final DatabaseType databaseType = configuration.databaseType;

		try {
			final Tuple2<PreparedStatement, Optional<String>> insertEntry =
					queries.prepareInsertEntryQuery(
							streamName,
							streamVersion,
							entry.entryData(),
							entry.typeName(),
							entry.typeVersion(),
							gson.toJson(entry.metadata()));

			if (insertEntry._1.executeUpdate() != 1) {
				logger.error("vlingo-symbio-jdbc:journal-" + databaseType + ": Could not insert event " + entry.toString());
				throw new IllegalStateException("vlingo-symbio-jdbc:journal-" + databaseType + ": Could not insert event");
			}

			if (insertEntry._2.isPresent()) {
				((BaseEntry<String>) entry).__internal__setId(String.valueOf(insertEntry._2.get()));
			} else {
				final long id = queries.generatedKeyFrom(insertEntry._1);
				if (id > 0) {
					((BaseEntry<String>) entry).__internal__setId(String.valueOf(id));
				}
			}
		} catch (final SQLException e) {
			postAppendAction.accept(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)));
			logger.error("vlingo-symbio-jdbc:journal-" + databaseType +": Could not insert event " + entry.toString(), e);
			throw new IllegalStateException(e);
		}
	}
}
