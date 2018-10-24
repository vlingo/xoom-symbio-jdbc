package io.vlingo.symbio.store.state.jdbc.postgres.eventjournal;

import com.google.gson.Gson;
import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;
import io.vlingo.symbio.Event;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.eventjournal.EventStream;
import io.vlingo.symbio.store.eventjournal.EventStreamReader;
import io.vlingo.symbio.store.state.jdbc.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresEventStreamReader<T> extends Actor implements EventStreamReader<T> {
    private static final String QUERY_EVENTS =
            "SELECT id, event_data, event_metadata, event_type, event_type_version " +
                    "FROM vlingo_event_journal " +
                    "WHERE event_stream = ? AND event_offset > ?";

    private static final String QUERY_SNAPSHOT =
            "SELECT id, snapshot_state_text, snapshot_state_binary, snapshot_state_type " +
                    "FROM vlingo_event_journal_snapshots " +
                    "WHERE event_stream = ?";

    private final Connection connection;
    private final PreparedStatement queryEventsStatement;
    private final PreparedStatement queryLatestSnapshotStatement;
    private final Gson gson;

    public PostgresEventStreamReader(final Configuration configuration) throws SQLException {
        this.connection = configuration.connection;
        this.queryEventsStatement = this.connection.prepareStatement(QUERY_EVENTS);
        this.queryLatestSnapshotStatement = this.connection.prepareStatement(QUERY_SNAPSHOT);
        this.gson = new Gson();
    }

    @Override
    public Completes<EventStream<T>> streamFor(final String streamName) {
        EventStream<T> result = null;

        try {
            final List<Event<T>> events = eventsFromOffset(streamName);
            result = new EventStream<>(streamName, 1, events, (State<T>) State.NullState.Text);
        } catch (Exception e) {
            logger().log("vlingo/symbio-postgresql: " + e.getMessage(), e);
        }

        return completes().with(result);
    }

    @Override
    public Completes<EventStream<T>> streamFor(final String streamName, final int fromStreamVersion) {
        return null;
    }

    private List<Event<T>> eventsFromOffset(final String streamName) throws Exception {
        final List<Event<T>> events = new ArrayList<>();

        queryEventsStatement.setString(1, streamName);
        queryEventsStatement.setInt(2, 0);
        final ResultSet resultSet = queryEventsStatement.executeQuery();
        while (resultSet.next()) {
            String id = resultSet.getString(1);
            String eventData = resultSet.getString(2);
            String eventMetadata = resultSet.getString(3);
            String eventType = resultSet.getString(4);
            int eventTypeVersion = resultSet.getInt(5);

            Class<T> classOfEvent = (Class<T>) Class.forName(eventType);
            Metadata eventMetadataDeserialized = gson.fromJson(eventMetadata, Metadata.class);

            events.add((Event<T>) new Event.TextEvent(id, classOfEvent, eventTypeVersion, eventData, eventMetadataDeserialized));
        }

        return events;
    }
}
