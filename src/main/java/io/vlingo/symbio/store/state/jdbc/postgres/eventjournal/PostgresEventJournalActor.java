package io.vlingo.symbio.store.state.jdbc.postgres.eventjournal;

import com.google.gson.Gson;
import io.vlingo.actors.Actor;
import io.vlingo.actors.Address;
import io.vlingo.actors.Definition;
import io.vlingo.common.Completes;
import io.vlingo.symbio.Event;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.eventjournal.EventJournal;
import io.vlingo.symbio.store.eventjournal.EventJournalListener;
import io.vlingo.symbio.store.eventjournal.EventJournalReader;
import io.vlingo.symbio.store.eventjournal.EventStreamReader;
import io.vlingo.symbio.store.state.jdbc.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PostgresEventJournalActor extends Actor implements EventJournal<String> {
    private static final String INSERT_EVENT =
            "INSERT INTO vlingo_event_journal(event_data, event_metadata, event_type, event_type_version, event_stream, event_offset)" +
                    "VALUES(?::JSON, ?::JSON, ?, ?, ?, ?)";

    private static final String INSERT_SNAPSHOT =
            "INSERT INTO vlingo_event_journal_snapshots(event_stream, snapshot_type, snapshot_type_version, snapshot_data, snapshot_data_version, snapshot_metadata)" +
                    "VALUES(?, ?, ?, ?::JSON, ?, ?::JSON)";

    private final Configuration configuration;
    private final Connection connection;
    private final EventJournalListener<String> listener;
    private final PreparedStatement insertEvent;
    private final PreparedStatement insertSnapshot;
    private final Gson gson;

    public PostgresEventJournalActor(Configuration configuration, EventJournalListener<String> listener) throws SQLException {
        this.configuration = configuration;
        this.connection = configuration.connection;
        this.listener = listener;

        this.insertEvent = connection.prepareStatement(INSERT_EVENT);
        this.insertSnapshot = connection.prepareStatement(INSERT_SNAPSHOT);

        this.gson = new Gson();
    }

    @Override
    public void append(String streamName, int streamVersion, Event<String> event) {
        insertEvent(streamName, streamVersion, event);
        listener.appended(event);
        doCommit();
    }

    @Override
    public void appendWith(String streamName, int streamVersion, Event<String> event, State<String> snapshot) {
        insertEvent(streamName, streamVersion, event);
        insertSnapshot(streamName, snapshot);
        listener.appendedWith(event, snapshot);
        doCommit();
    }

    @Override
    public void appendAll(String streamName, int fromStreamVersion, List<Event<String>> events) {
        AtomicInteger version = new AtomicInteger(fromStreamVersion);
        events.forEach(event -> insertEvent(streamName, version.getAndIncrement(), event));
        listener.appendedAll(events);
        doCommit();
    }

    @Override
    public void appendAllWith(String streamName, int fromStreamVersion, List<Event<String>> events, State<String> snapshot) {
        AtomicInteger version = new AtomicInteger(fromStreamVersion);
        events.forEach(event -> insertEvent(streamName, version.getAndIncrement(), event));
        insertSnapshot(streamName, snapshot);
        listener.appendedAllWith(events, snapshot);
        doCommit();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Completes<EventJournalReader<String>> eventJournalReader(String name) {
        Address address = stage().world().addressFactory().uniquePrefixedWith("eventJournalReader-" + name);
        EventJournalReader<String> reader = stage().actorFor(
                Definition.has(
                        PostgresEventJournalReaderActor.class,
                        Definition.parameters(configuration, name)
                ),
                EventJournalReader.class,
                address
        );

        return completes().with(reader);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Completes<EventStreamReader<String>> eventStreamReader(String name) {
        Address address = stage().world().addressFactory().uniquePrefixedWith("eventStreamReader-" + name);
        EventStreamReader<String> reader = stage().actorFor(
                Definition.has(
                        PostgresEventStreamReaderActor.class,
                        Definition.parameters(configuration)),
                EventStreamReader.class,
                address
        );

        return completes().with(reader);
    }

    protected final void insertEvent(final String eventStream, final int eventVersion, final Event<String> event) {
        try {
            insertEvent.setString(1, event.eventData);
            insertEvent.setString(2, gson.toJson(event.metadata));
            insertEvent.setString(3, event.type);
            insertEvent.setInt(4, event.typeVersion);
            insertEvent.setString(5, eventStream);
            insertEvent.setInt(6, eventVersion);

            if (insertEvent.executeUpdate() != 1) {
                logger().log("vlingo/symbio-postgres: Could not insert event " + event.toString());
            }
        } catch (SQLException e) {
            logger().log("vlingo/symbio-postgres: Could not insert event " + event.toString());
        }
    }

    protected final void insertSnapshot(final String eventStream, final State<String> snapshot) {
        try {
            insertSnapshot.setString(1, eventStream);
            insertSnapshot.setString(2, snapshot.type);
            insertSnapshot.setInt(3, snapshot.typeVersion);
            insertSnapshot.setString(4, snapshot.data);
            insertSnapshot.setInt(5, snapshot.dataVersion);
            insertSnapshot.setString(6, gson.toJson(snapshot.metadata));

            if (insertSnapshot.executeUpdate() != 1) {
                logger().log("vlingo/symbio-postgres: Could not insert event with id " + snapshot.id);
            }
        } catch (SQLException e) {
            logger().log("vlingo/symbio-postgres: Could not insert event with id " + snapshot.id);
        }
    }

    private void doCommit() {
        try {
            connection.commit();
        } catch (SQLException e) {
            logger().log("vlingo/symbio-postgres: Could not complete transaction", e);
        }
    }
}
