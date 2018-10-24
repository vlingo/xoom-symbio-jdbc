package io.vlingo.symbio.store.state.jdbc.postgres.eventjournal;

import com.google.gson.Gson;
import io.vlingo.actors.Definition;
import io.vlingo.actors.World;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.eventjournal.EventStream;
import io.vlingo.symbio.store.eventjournal.EventStreamReader;
import io.vlingo.symbio.store.state.StateStore.DataFormat;
import io.vlingo.symbio.store.state.jdbc.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vlingo.symbio.store.state.jdbc.postgres.PostgresConfigurationProvider.testConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PostgresEventStreamReaderTest {
    private static final String CREATE_TABLE =
            "CREATE TABLE vlingo_event_journal(" +
                    "id BIGSERIAL PRIMARY KEY," +
                    "event_data VARCHAR(256) NOT NULL," +
                    "event_metadata VARCHAR(256) NOT NULL," +
                    "event_type VARCHAR(256) NOT NULL," +
                    "event_type_version INTEGER," +
                    "event_stream VARCHAR(128)," +
                    "event_offset INTEGER" +
                    ")";

    private static final String DROP_TABLE =
            "DROP TABLE vlingo_event_journal";

    private static final String INSERT_EVENT =
            "INSERT INTO vlingo_event_journal(event_data, event_metadata, event_type, event_type_version, event_stream, event_offset)" +
                    "VALUES(?, '', ?, 1, ?, (SELECT MAX(event_offset) + 1 FROM vlingo_event_journal))";

    private EventStreamReader<AggregateRoot> eventStreamReader;
    private Configuration configuration;
    private World world;
    private String aggregateRootId;
    private Gson gson;
    private String streamName;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        streamName = AggregateRoot.class.getCanonicalName();
        world = World.startWithDefaults("event-stream-tests");

        configuration = testConfiguration(DataFormat.Text);
        eventStreamReader = world.actorFor(
                Definition.has(PostgresEventStreamReader.class,
                        Definition.parameters(configuration)),
                EventStreamReader.class
        );

        aggregateRootId = UUID.randomUUID().toString();
        gson = new Gson();
        setUpDatabase();
    }

    @After
    public void tearDown() throws Exception {
        dropDatabase();
        world.terminate();
    }

    @Test
    public void testThatCanReadAllEventsFromJournal() throws Exception {
        insertEvent(1);
        insertEvent(2);
        insertEvent(3);

        EventStream<AggregateRoot> stream = eventStreamReader.streamFor(streamName).await();
        assertEquals(State.NullState.Text, stream.snapshot);
        assertEquals(stream.streamVersion, 1);
        assertEquals(stream.streamName, streamName);

        AtomicInteger eventNumber = new AtomicInteger(1);
        stream.events.forEach(event -> {
            assertEquals(eventNumber.getAndIncrement(), event.eventData.number);
        });
    }

    private void setUpDatabase() throws SQLException {
        try (
                final PreparedStatement createTable = configuration.connection.prepareStatement(CREATE_TABLE)
        ) {
            createTable.executeUpdate();
        }
    }

    private void dropDatabase() throws SQLException {
        try (
                final PreparedStatement dropTable = configuration.connection.prepareStatement(DROP_TABLE)
        ) {
            dropTable.executeUpdate();
        }
    }

    private void insertEvent(final int number) throws SQLException {
        try (final PreparedStatement stmt = configuration.connection.prepareStatement(INSERT_EVENT)) {
            stmt.setString(1, gson.toJson(new AggregateRoot(aggregateRootId, number)));
            stmt.setString(2, streamName);
            stmt.setString(3, aggregateRootId);

            assert stmt.executeUpdate() == 1;
        }
    }

    public static class AggregateRoot {
        public final String id;
        public final int number;

        public AggregateRoot(String id, int number) {
            this.id = id;
            this.number = number;
        }
    }
}