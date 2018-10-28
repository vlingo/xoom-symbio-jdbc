package io.vlingo.symbio.store.state.jdbc.postgres.eventjournal;

import com.google.gson.Gson;
import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;
import io.vlingo.symbio.Event;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.eventjournal.EventJournalReader;
import io.vlingo.symbio.store.eventjournal.EventStream;
import io.vlingo.symbio.store.state.jdbc.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PostgresEventJournalReader<T> extends Actor implements EventJournalReader<T> {
    private static final String QUERY_CURRENT_OFFSET =
            "SELECT reader_offset FROM vlingo_event_journal_offsets WHERE reader_name=?";

    private static final String UPDATE_CURRENT_OFFSET =
            "UPDATE vlingo_event_journal_offsets SET reader_offset=? WHERE reader_name=?";

    private static final String QUERY_SINGLE =
            "SELECT * FROM vlingo_event_journal WHERE id = ?";

    private static final String QUERY_BATCH =
            "SELECT * FROM vlingo_event_journal WHERE id BETWEEN ? AND ?";

    private final Connection connection;
    private final String name;
    private final PreparedStatement queryCurrentOffset;
    private final PreparedStatement updateCurrentOffset;
    private final PreparedStatement querySingleEvent;
    private final PreparedStatement queryEventBatch;
    private final Gson gson;

    private int offset;

    public PostgresEventJournalReader(final Configuration configuration, final String name) throws SQLException {
        this.connection = configuration.connection;
        this.name = name;

        this.queryCurrentOffset = this.connection.prepareStatement(QUERY_CURRENT_OFFSET);
        this.updateCurrentOffset = this.connection.prepareStatement(UPDATE_CURRENT_OFFSET);
        this.querySingleEvent = this.connection.prepareStatement(QUERY_SINGLE);
        this.queryEventBatch = this.connection.prepareStatement(QUERY_BATCH);

        this.gson = new Gson();
        retrieveCurrentOffset();
    }

    @Override
    public Completes<String> name() {
        return completes().with(name);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Completes<Event<T>> readNext() {
        try {
            querySingleEvent.setInt(1, offset);
            final ResultSet resultSet = querySingleEvent.executeQuery();
            if (resultSet.next()) {
                offset += 1;
                updateCurrentOffset();
                return completes()
                        .with(eventFromResultSet(resultSet));
            }
        } catch (Exception e) {
            logger().log("vlingo/symbio-postgres: " + e.getMessage(), e);
        }

        return completes().with(null);
    }

    @Override
    public Completes<EventStream<T>> readNext(int maximumEvents) {
        try {
            List<Event<T>> events = new ArrayList<>(maximumEvents);
            queryEventBatch.setInt(1, offset);
            queryEventBatch.setInt(2, offset + maximumEvents - 1);

            final ResultSet resultSet = queryEventBatch.executeQuery();
            while (resultSet.next()) {
                offset += 1;
                events.add(eventFromResultSet(resultSet));
            }

            updateCurrentOffset();
            return completes().with(new EventStream<>(name, offset, events, (State<T>) State.NullState.Text));

        } catch (Exception e) {
            logger().log("vlingo/symbio-postgres: " + e.getMessage(), e);
        }

        return completes().with(null);
    }

    @Override
    public void rewind() {
        this.offset = 1;
        updateCurrentOffset();
    }

    @Override
    public Completes<String> seekTo(String id) {
        return null;
    }

    private Event<T> eventFromResultSet(ResultSet resultSet) throws SQLException, ClassNotFoundException {
        final String id = resultSet.getString(1);
        final String eventData = resultSet.getString(2);
        final String eventMetadata = resultSet.getString(3);
        final String eventType = resultSet.getString(4);
        final int eventTypeVersion = resultSet.getInt(5);

        final Class<T> classOfEvent = (Class<T>) Class.forName(eventType);
        final T eventDataDeserialized = gson.fromJson(eventData, classOfEvent);

        final Metadata eventMetadataDeserialized = gson.fromJson(eventMetadata, Metadata.class);
        return (Event<T>) new Event.ObjectEvent<>(id, classOfEvent, eventTypeVersion, eventDataDeserialized, eventMetadataDeserialized);
    }

    private void retrieveCurrentOffset() {
        this.offset = 1;

        try {
            queryCurrentOffset.setString(1, name);
            final ResultSet resultSet = queryCurrentOffset.executeQuery();
            if (resultSet.next()) {
                this.offset = resultSet.getInt(1);
            }
        } catch (Exception e) {
            logger().log("vlingo/symbio-postgres: " + e.getMessage(), e);
            logger().log("vlingo/symbio-postgres: Rewinding the offset");
        }
    }

    private void updateCurrentOffset() {
        try {
            updateCurrentOffset.setInt(1, offset);
            updateCurrentOffset.setString(2, name);

            updateCurrentOffset.executeUpdate();
        } catch (Exception e) {
            logger().log("vlingo/symbio-postgres: Could not persist the offset. Will retry on next read.");
            logger().log("vlingo/symbio-postgres: " + e.getMessage(), e);
        }
    }
}
