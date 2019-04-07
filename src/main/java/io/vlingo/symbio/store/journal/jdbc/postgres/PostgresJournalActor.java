// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;

import com.google.gson.Gson;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Address;
import io.vlingo.actors.Definition;
import io.vlingo.common.Completes;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.common.Tuple2;
import io.vlingo.common.identity.IdentityGenerator;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.BaseEntry.TextEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapter;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.StateAdapter;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.journal.Journal;
import io.vlingo.symbio.store.journal.JournalListener;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.journal.StreamReader;

public class PostgresJournalActor extends Actor implements Journal<String> {
    private static final String INSERT_EVENT =
            "INSERT INTO vlingo_symbio_journal(entry_data, entry_metadata, entry_type, entry_type_version, stream_name, stream_version, id, entry_timestamp)" +
                    "VALUES(?::JSONB, ?::JSONB, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_SNAPSHOT =
            "INSERT INTO vlingo_symbio_journal_snapshots(stream_name, snapshot_type, snapshot_type_version, snapshot_data, snapshot_data_version, snapshot_metadata)" +
                    "VALUES(?, ?, ?, ?::JSONB, ?, ?::JSONB)";

    private final Map<Class<?>,EntryAdapter<? extends Source<?>,? extends Entry<?>>> sourceAdapters;
    private final Map<Class<?>,StateAdapter<?,?>> stateAdapters;
    private final Configuration configuration;
    private final Connection connection;
    private final JournalListener<String> listener;
    private final PreparedStatement insertEvent;
    private final PreparedStatement insertSnapshot;
    private final Gson gson;
    private final Map<String, JournalReader<TextEntry>> journalReaders;
    private final Map<String, StreamReader<String>> streamReaders;
    private final IdentityGenerator identityGenerator;

    public PostgresJournalActor(JournalListener<String> listener, Configuration configuration) throws SQLException {
        this.configuration = configuration;
        this.connection = configuration.connection;
        this.listener = listener;

        this.insertEvent = connection.prepareStatement(INSERT_EVENT);
        this.insertSnapshot = connection.prepareStatement(INSERT_SNAPSHOT);

        this.gson = new Gson();

        this.sourceAdapters = new HashMap<>();
        this.stateAdapters = new HashMap<>();
        this.journalReaders = new HashMap<>();
        this.streamReaders = new HashMap<>();

        this.identityGenerator = new IdentityGenerator.TimeBasedIdentityGenerator();
    }

    @Override
    public <S,ST> void append(final String streamName, final int streamVersion, final Source<S> source, final AppendResultInterest interest, final Object object) {
      final Consumer<Exception> whenFailed =
              (e) -> appendResultedInFailure(streamName, streamVersion, source, null, interest, object, e);
      final Entry<String> entry = asEntry(source, whenFailed);
      insertEntry(streamName, streamVersion, entry, whenFailed);
      doCommit(whenFailed);
      listener.appended(entry);
      interest.appendResultedIn(Success.of(Result.Success), streamName, streamVersion, source, Optional.empty(), object);
    }

    @Override
    public <S,ST> void appendWith(final String streamName, final int streamVersion, final Source<S> source, final ST snapshot, final AppendResultInterest interest, final Object object) {
      final Consumer<Exception> whenFailed =
              (e) -> appendResultedInFailure(streamName, streamVersion, source, snapshot, interest, object, e);
      final Entry<String> entry = asEntry(source, whenFailed);
      insertEntry(streamName, streamVersion, entry, whenFailed);
      final Tuple2<Optional<ST>,Optional<TextState>> snapshotState = toState(snapshot, streamVersion);
      snapshotState._2.ifPresent(state -> insertSnapshot(streamName, state, whenFailed));
      doCommit(whenFailed);
      listener.appendedWith(entry, snapshotState._2.orElseGet(() -> null));
      interest.appendResultedIn(Success.of(Result.Success), streamName, streamVersion, source, snapshotState._1, object);
    }


    @Override
    public <S,ST> void appendAll(final String streamName, final int fromStreamVersion, final List<Source<S>> sources, final AppendResultInterest interest, final Object object) {
      final Consumer<Exception> whenFailed =
              (e) -> appendAllResultedInFailure(streamName, fromStreamVersion, sources, null, interest, object, e);
      final List<Entry<String>> entries = asEntries(sources, whenFailed);
      int version = fromStreamVersion;
      for (Entry<String> entry : entries) {
          insertEntry(streamName, version++, entry, whenFailed);
      }
      doCommit(whenFailed);
      listener.appendedAll(entries);
      interest.appendAllResultedIn(Success.of(Result.Success), streamName, fromStreamVersion, sources, Optional.empty(), object);
    }

    @Override
    public <S,ST> void appendAllWith(final String streamName, final int fromStreamVersion, final List<Source<S>> sources, final ST snapshot, final AppendResultInterest interest, final Object object) {
      final Consumer<Exception> whenFailed =
              (e) -> appendAllResultedInFailure(streamName, fromStreamVersion, sources, snapshot, interest, object, e);
      final List<Entry<String>> entries = asEntries(sources, whenFailed);
      int version = fromStreamVersion;
      for (Entry<String> entry : entries) {
        insertEntry(streamName, version++, entry, whenFailed);
      }
      final Tuple2<Optional<ST>,Optional<TextState>> snapshotState = toState(snapshot, fromStreamVersion);
      snapshotState._2.ifPresent(state -> insertSnapshot(streamName, state, whenFailed));
      doCommit(whenFailed);
      listener.appendedAllWith(entries, snapshotState._2.orElseGet(() -> null));
      interest.appendAllResultedIn(Success.of(Result.Success), streamName, fromStreamVersion, sources, snapshotState._1, object);
    }

    @Override
    public <S extends Source<?>,E extends Entry<?>> void registerEntryAdapter(final Class<S> sourceType, final EntryAdapter<S,E> adapter) {
      sourceAdapters.put(sourceType, adapter);
    }

    @Override
    public <S,R extends State<?>> void registerStateAdapter(Class<S> stateType, StateAdapter<S,R> adapter) {
      stateAdapters.put(stateType, adapter);
    }

    @SuppressWarnings("unchecked")
    private <S extends Source<?>,E extends Entry<?>> EntryAdapter<S,E> adapter(final Class<S> sourceType) {
      final EntryAdapter<S,E> adapter = (EntryAdapter<S,E>) sourceAdapters.get(sourceType);
      if (adapter != null) {
        return adapter;
      }
      throw new IllegalStateException("Adapter not registrered for: " + sourceType.getName());
    }

    private <S> List<Entry<String>> asEntries(final List<Source<S>> sources, final Consumer<Exception> whenFailed) {
      final List<Entry<String>> entries = new ArrayList<>();
      for (final Source<?> source : sources) {
        entries.add(asEntry(source, whenFailed));
      }
      return entries;
    }

    @SuppressWarnings("unchecked")
    private Entry<String> asEntry(final Source<?> source, final Consumer<Exception> whenFailed) {
      try {
        final EntryAdapter<Source<?>,Entry<?>>  adapter = (EntryAdapter<Source<?>,Entry<?>>) adapter(source.getClass());

        return (Entry<String>) adapter.toEntry(source);
      } catch (Exception e) {
        whenFailed.accept(e);
        logger().log("vlingo/symbio-jdbc-postgres: Cannot adapt source to entry because: ", e);
        throw new IllegalArgumentException(e);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Completes<JournalReader<TextEntry>> journalReader(String name) {
        final JournalReader<TextEntry> reader = journalReaders.computeIfAbsent(name, (key) -> {
            Address address = stage().world().addressFactory().uniquePrefixedWith("eventJournalReader-" + name);
            return stage().actorFor(
                    JournalReader.class,
                    Definition.has(
                            PostgresJournalReaderActor.class,
                            Definition.parameters(configuration, name)
                    ),
                    address
            );
        });

        return completes().with(reader);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Completes<StreamReader<String>> streamReader(String name) {
        final StreamReader<String> reader = streamReaders.computeIfAbsent(name, (key) -> {
            Address address = stage().world().addressFactory().uniquePrefixedWith("eventStreamReader-" + key);
            return stage().actorFor(
                    StreamReader.class,
                    Definition.has(
                            PostgresStreamReaderActor.class,
                            Definition.parameters(configuration)),
                    address
            );
        });


        return completes().with(reader);
    }

    protected final void insertEntry(
            final String streamName,
            final int streamVersion,
            final Entry<String> entry,
            final Consumer<Exception> whenFailed) {
        try {
            final UUID id = identityGenerator.generate();
            final long timestamp = id.timestamp();

            insertEvent.setString(1, entry.entryData());
            insertEvent.setString(2, gson.toJson(entry.metadata()));
            insertEvent.setString(3, entry.type());
            insertEvent.setInt(4, entry.typeVersion());
            insertEvent.setString(5, streamName);
            insertEvent.setInt(6, streamVersion);
            insertEvent.setObject(7, id);
            insertEvent.setLong(8, timestamp);

            if (insertEvent.executeUpdate() != 1) {
                logger().log("vlingo/symbio-jdbc-postgres: Could not insert event " + entry.toString());
                throw new IllegalStateException("vlingo/symbio-jdbc-postgres: Could not insert event");
            }

            ((BaseEntry<String>) entry).__internal__setId(id.toString()); //questionable cast
        } catch (SQLException e) {
            whenFailed.accept(e);
            logger().log("vlingo/symbio-jdbc-postgres: Could not insert event " + entry.toString(), e);
            throw new IllegalStateException(e);
        }
    }

    protected final void insertSnapshot(
            final String eventStream,
            final TextState snapshotState,
            final Consumer<Exception> whenFailed) {
        try {
            insertSnapshot.setString(1, eventStream);
            insertSnapshot.setString(2, snapshotState.type);
            insertSnapshot.setInt(3, snapshotState.typeVersion);
            insertSnapshot.setString(4, snapshotState.data);
            insertSnapshot.setInt(5, snapshotState.dataVersion);
            insertSnapshot.setString(6, gson.toJson(snapshotState.metadata));

            if (insertSnapshot.executeUpdate() != 1) {
                logger().log("vlingo/symbio-jdbc-postgres: Could not insert snapshot with id " + snapshotState.id);
                throw new IllegalStateException("vlingo/symbio-jdbc-postgres: Could not insert snapshot");
            }
        } catch (SQLException e) {
            whenFailed.accept(e);
            logger().log("vlingo/symbio-jdbc-postgres: Could not insert event with id " + snapshotState.id, e);
            throw new IllegalStateException(e);
        }
    }

    private <S,ST> void appendResultedInFailure(
            String streamName,
            int streamVersion,
            Source<S> source,
            final ST snapshot,
            AppendResultInterest interest,
            Object object,
            Exception e) {

      interest.appendResultedIn(
              Failure.of(new StorageException(Result.Failure, e.getMessage(), e)),
              streamName,
              streamVersion,
              source,
              snapshot == null ? Optional.empty() : Optional.of(snapshot),
              object);
    }

    private <S,ST> void appendAllResultedInFailure(
            String streamName,
            int streamVersion,
            List<Source<S>> sources,
            final ST snapshot,
            AppendResultInterest interest,
            Object object,
            Exception e) {

      interest.appendAllResultedIn(
              Failure.of(new StorageException(Result.Failure, e.getMessage(), e)),
              streamName,
              streamVersion,
              sources,
              snapshot == null ? Optional.empty() : Optional.of(snapshot),
              object);
    }

    private void doCommit(final Consumer<Exception> whenFailed) {
        try {
            connection.commit();
        } catch (SQLException e) {
            whenFailed.accept(e);
            logger().log("vlingo/symbio-jdbc-postgres: Could not complete transaction", e);
            throw new IllegalStateException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private <ST> Tuple2<Optional<ST>,Optional<TextState>> toState(final ST snapshot, final int streamVersion) {
        if (snapshot != null) {
            final StateAdapter<ST,TextState> adapter = (StateAdapter<ST,TextState>) stateAdapters.get(snapshot.getClass());
            return Tuple2.from(Optional.of(snapshot), Optional.of(adapter.toRawState(snapshot, streamVersion)));
        } else {
            return Tuple2.from(Optional.empty(), Optional.empty());
        }
    }
}
