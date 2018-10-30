// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.postgres.eventjournal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import com.google.gson.Gson;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Address;
import io.vlingo.actors.Definition;
import io.vlingo.common.Completes;
import io.vlingo.common.identity.IdentityGenerator;
import io.vlingo.symbio.Event;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.eventjournal.EventJournal;
import io.vlingo.symbio.store.eventjournal.EventJournalListener;
import io.vlingo.symbio.store.eventjournal.EventJournalReader;
import io.vlingo.symbio.store.eventjournal.EventStreamReader;
import io.vlingo.symbio.store.state.jdbc.Configuration;

public class PostgresEventJournalActor extends Actor implements EventJournal<String> {
    private static final String INSERT_EVENT =
            "INSERT INTO vlingo_event_journal(event_data, event_metadata, event_type, event_type_version, event_stream, event_offset, id, event_timestamp)" +
                    "VALUES(?::JSONB, ?::JSONB, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_SNAPSHOT =
            "INSERT INTO vlingo_event_journal_snapshots(event_stream, snapshot_type, snapshot_type_version, snapshot_data, snapshot_data_version, snapshot_metadata)" +
                    "VALUES(?, ?, ?, ?::JSONB, ?, ?::JSONB)";

    private final Configuration configuration;
    private final Connection connection;
    private final EventJournalListener<String> listener;
    private final PreparedStatement insertEvent;
    private final PreparedStatement insertSnapshot;
    private final Gson gson;
    private final Map<String, EventJournalReader<String>> journalReaders;
    private final Map<String, EventStreamReader<String>> streamReaders;
    private final IdentityGenerator identityGenerator;

    public PostgresEventJournalActor(Configuration configuration, EventJournalListener<String> listener) throws SQLException {
        this.configuration = configuration;
        this.connection = configuration.connection;
        this.listener = listener;

        this.insertEvent = connection.prepareStatement(INSERT_EVENT);
        this.insertSnapshot = connection.prepareStatement(INSERT_SNAPSHOT);

        this.gson = new Gson();

        this.journalReaders = new HashMap<>();
        this.streamReaders = new HashMap<>();

        this.identityGenerator = new IdentityGenerator.TimeBasedIdentityGenerator();
    }

    @Override
    public void append(final String streamName, final int streamVersion, final Event<String> event, final AppendResultInterest<String> interest, final Object object) {
        final Consumer<Exception> whenFailed =
                (e) -> interest.appendResultedIn(Result.Failure, e, streamName, streamVersion, event, object);
        insertEvent(streamName, streamVersion, event, whenFailed);
        doCommit(whenFailed);
        listener.appended(event);
        interest.appendResultedIn(Result.Success, streamName, streamVersion, event, object);
    }

    @Override
    public void appendWith(final String streamName, final int streamVersion, final Event<String> event, final State<String> snapshot, final AppendResultInterest<String> interest, final Object object) {
      final Consumer<Exception> whenFailed =
                (e) -> interest.appendResultedIn(Result.Failure, e, streamName, streamVersion, event, object);
        insertEvent(streamName, streamVersion, event, whenFailed);
        insertSnapshot(streamName, snapshot, whenFailed);
        doCommit(whenFailed);
        listener.appendedWith(event, snapshot);
        interest.appendResultedIn(Result.Success, streamName, streamVersion, event, snapshot, object);
    }

    @Override
    public void appendAll(final String streamName, final int fromStreamVersion, final List<Event<String>> events, final AppendResultInterest<String> interest, final Object object) {
      final Consumer<Exception> whenFailed =
                (e) -> interest.appendResultedIn(Result.Failure, e, streamName, fromStreamVersion, events, object);
        int version = fromStreamVersion;
        for (Event<String> event : events) {
            insertEvent(streamName, version++, event, whenFailed);
        }
        doCommit(whenFailed);
        listener.appendedAll(events);
        interest.appendResultedIn(Result.Success, streamName, fromStreamVersion, events, object);
    }

    @Override
    public void appendAllWith(final String streamName, final int fromStreamVersion, final List<Event<String>> events, final State<String> snapshot, final AppendResultInterest<String> interest, final Object object) {
      final Consumer<Exception> whenFailed =
                (e) -> interest.appendResultedIn(Result.Failure, e, streamName, fromStreamVersion, events, object);
        int version = fromStreamVersion;
        for (Event<String> event : events) {
            insertEvent(streamName, version++, event, whenFailed);
        }
        insertSnapshot(streamName, snapshot, whenFailed);
        doCommit(whenFailed);
        listener.appendedAllWith(events, snapshot);
        interest.appendResultedIn(Result.Success, streamName, fromStreamVersion, events, snapshot, object);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Completes<EventJournalReader<String>> eventJournalReader(String name) {
        final EventJournalReader<String> reader = journalReaders.computeIfAbsent(name, (key) -> {
            Address address = stage().world().addressFactory().uniquePrefixedWith("eventJournalReader-" + name);
            return stage().actorFor(
                    Definition.has(
                            PostgresEventJournalReaderActor.class,
                            Definition.parameters(configuration, name)
                    ),
                    EventJournalReader.class,
                    address
            );
        });

        return completes().with(reader);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Completes<EventStreamReader<String>> eventStreamReader(String name) {
        final EventStreamReader<String> reader = streamReaders.computeIfAbsent(name, (key) -> {
            Address address = stage().world().addressFactory().uniquePrefixedWith("eventStreamReader-" + key);
            return stage().actorFor(
                    Definition.has(
                            PostgresEventStreamReaderActor.class,
                            Definition.parameters(configuration)),
                    EventStreamReader.class,
                    address
            );
        });


        return completes().with(reader);
    }

    protected final void insertEvent(
            final String eventStream,
            final int eventVersion,
            final Event<String> event,
            final Consumer<Exception> whenFailed) {
        try {
            final UUID id = identityGenerator.generate();
            final long timestamp = id.timestamp();

            insertEvent.setString(1, event.eventData);
            insertEvent.setString(2, gson.toJson(event.metadata));
            insertEvent.setString(3, event.type);
            insertEvent.setInt(4, event.typeVersion);
            insertEvent.setString(5, eventStream);
            insertEvent.setInt(6, eventVersion);
            insertEvent.setObject(7, id);
            insertEvent.setLong(8, timestamp);

            if (insertEvent.executeUpdate() != 1) {
                logger().log("vlingo/symbio-postgres: Could not insert event " + event.toString());
                throw new IllegalStateException("vlingo/symbio-postgres: Could not insert event");
            }

            event.__internal__setId(id.toString());
        } catch (SQLException e) {
            whenFailed.accept(e);
            logger().log("vlingo/symbio-postgres: Could not insert event " + event.toString(), e);
            throw new IllegalStateException(e);
        }
    }

    protected final void insertSnapshot(
            final String eventStream,
            final State<String> snapshot,
            final Consumer<Exception> whenFailed) {
        try {
            insertSnapshot.setString(1, eventStream);
            insertSnapshot.setString(2, snapshot.type);
            insertSnapshot.setInt(3, snapshot.typeVersion);
            insertSnapshot.setString(4, snapshot.data);
            insertSnapshot.setInt(5, snapshot.dataVersion);
            insertSnapshot.setString(6, gson.toJson(snapshot.metadata));

            if (insertSnapshot.executeUpdate() != 1) {
                logger().log("vlingo/symbio-postgres: Could not insert event with id " + snapshot.id);
                throw new IllegalStateException("vlingo/symbio-postgres: Could not insert event");
            }
        } catch (SQLException e) {
            whenFailed.accept(e);
            logger().log("vlingo/symbio-postgres: Could not insert event with id " + snapshot.id, e);
            throw new IllegalStateException(e);
        }
    }

    private void doCommit(final Consumer<Exception> whenFailed) {
        try {
            connection.commit();
        } catch (SQLException e) {
            whenFailed.accept(e);
            logger().log("vlingo/symbio-postgres: Could not complete transaction", e);
            throw new IllegalStateException(e);
        }
    }
}
