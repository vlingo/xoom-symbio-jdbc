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

import static java.util.Collections.emptyList;

public class PostgresEventStreamReaderActor extends Actor implements EventStreamReader<String> {
    private static final String QUERY_EVENTS =
            "SELECT id, event_data, event_metadata, event_type, event_type_version " +
                    "FROM vlingo_event_journal " +
                    "WHERE stream_name = ? AND stream_version >= ?";

    private static final String QUERY_SNAPSHOT =
            "SELECT snapshot_type, snapshot_type_version, snapshot_data, snapshot_data_version, snapshot_metadata " +
                    "FROM vlingo_event_journal_snapshots " +
                    "WHERE stream_name = ?";

    private final Connection connection;
    private final PreparedStatement queryEventsStatement;
    private final PreparedStatement queryLatestSnapshotStatement;
    private final Gson gson;

    public PostgresEventStreamReaderActor(final Configuration configuration) throws SQLException {
        this.connection = configuration.connection;
        this.queryEventsStatement = this.connection.prepareStatement(QUERY_EVENTS);
        this.queryLatestSnapshotStatement = this.connection.prepareStatement(QUERY_SNAPSHOT);
        this.gson = new Gson();
    }

    @Override
    public Completes<EventStream<String>> streamFor(final String streamName) {
        return streamFor(streamName, 1);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Completes<EventStream<String>> streamFor(final String streamName, final int fromStreamVersion) {
        try {
            return completes().with(eventsFromOffset(streamName, fromStreamVersion));
        } catch (Exception e) {
            logger().log("vlingo/symbio-postgresql: " + e.getMessage(), e);
            return completes().with(new EventStream<>(streamName, 1, emptyList(), State.NullState.Text));
        }
    }

    @SuppressWarnings("unchecked")
    private EventStream<String> eventsFromOffset(final String streamName, final int offset) throws Exception {
        final State<String> snapshot = latestSnapshotOf(streamName);
        final List<Event<String>> events = new ArrayList<>();

        int dataVersion = offset;
        State<String> referenceSnapshot = State.NullState.Text;

        if (snapshot != State.NullState.Text) {
            if (snapshot.dataVersion > offset) {
                dataVersion = snapshot.dataVersion + 1;
                referenceSnapshot = snapshot;
            }
        }

        queryEventsStatement.setString(1, streamName);
        queryEventsStatement.setInt(2, dataVersion);
        final ResultSet resultSet = queryEventsStatement.executeQuery();
        while (resultSet.next()) {
            final String id = resultSet.getString(1);
            final String eventData = resultSet.getString(2);
            final String eventMetadata = resultSet.getString(3);
            final String eventType = resultSet.getString(4);
            final int eventTypeVersion = resultSet.getInt(5);

            final Class<?> classOfEvent = Class.forName(eventType);
            final Metadata eventMetadataDeserialized = gson.fromJson(eventMetadata, Metadata.class);

            events.add(new Event.TextEvent(id, classOfEvent, eventTypeVersion, eventData, eventMetadataDeserialized));
        }

        return new EventStream<>(streamName, dataVersion + events.size(), events, referenceSnapshot);
    }

    @SuppressWarnings("unchecked")
    private State<String> latestSnapshotOf(final String streamName) throws Exception {
        queryLatestSnapshotStatement.setString(1, streamName);
        final ResultSet resultSet = queryLatestSnapshotStatement.executeQuery();
        if (resultSet.next()) {
            final String snapshotDataType = resultSet.getString(1);
            final int snapshotTypeVersion = resultSet.getInt(2);
            final String snapshotData = resultSet.getString(3);
            final int snapshotDataVersion = resultSet.getInt(4);
            final String metadataJson = resultSet.getString(5);

            final Class<?> classOfEvent = Class.forName(snapshotDataType);
            final Metadata eventMetadataDeserialized = gson.fromJson(metadataJson, Metadata.class);

            return new State.TextState(streamName, classOfEvent, snapshotTypeVersion, snapshotData, snapshotDataVersion, eventMetadataDeserialized);
        }

        return State.NullState.Text;
    }
}
