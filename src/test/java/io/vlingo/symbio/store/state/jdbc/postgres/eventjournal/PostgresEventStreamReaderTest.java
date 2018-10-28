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
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static io.vlingo.symbio.store.state.jdbc.postgres.PostgresConfigurationProvider.testConfiguration;
import static org.junit.Assert.assertEquals;

public class PostgresEventStreamReaderTest {
    private static final String EVENT_TABLE =
            "CREATE TABLE vlingo_event_journal(" +
                    "id BIGSERIAL PRIMARY KEY," +
                    "event_data JSON NOT NULL," +
                    "event_metadata JSON NOT NULL," +
                    "event_type VARCHAR(256) NOT NULL," +
                    "event_type_version INTEGER NOT NULL," +
                    "event_stream VARCHAR(128) NOT NULL," +
                    "event_offset INTEGER NOT NULL" +
                    ")";

    private static final String SNAPSHOT_TABLE =
            "CREATE TABLE vlingo_event_journal_snapshots(" +
                    "event_stream VARCHAR(128) PRIMARY KEY," +
                    "snapshot_type VARCHAR(256) NOT NULL," +
                    "snapshot_type_version INTEGER NOT NULL," +
                    "snapshot_data JSON NOT NULL," +
                    "snapshot_data_version INTEGER NOT NULL," +
                    "snapshot_metadata JSON NOT NULL" +
                    ")";

    private static final String DROP_EVENT_TABLE = "DROP TABLE vlingo_event_journal";
    private static final String DROP_SNAPSHOT_TABLE = "DROP TABLE vlingo_event_journal_snapshots";

    private static final String INSERT_EVENT =
            "INSERT INTO vlingo_event_journal(event_data, event_metadata, event_type, event_type_version, event_stream, event_offset)" +
                    "VALUES(?::JSON, '{}'::JSON, ?, 1, ?, (SELECT COALESCE(MAX(event_offset), 0) + 1 FROM vlingo_event_journal))";

    private static final String INSERT_SNAPSHOT =
            "INSERT INTO vlingo_event_journal_snapshots(event_stream, snapshot_type, snapshot_type_version, snapshot_data, snapshot_data_version, snapshot_metadata)" +
                    "VALUES(?, ?, 1, ?::JSON, ?, '{}'::JSON)";

    private EventStreamReader<AggregateRoot> eventStreamReader;
    private Configuration configuration;
    private World world;
    private String aggregateRootId;
    private Gson gson;
    private String streamName;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        aggregateRootId = UUID.randomUUID().toString();
        streamName = aggregateRootId;

        world = World.startWithDefaults("event-stream-tests");

        configuration = testConfiguration(DataFormat.Text);
        eventStreamReader = world.actorFor(
                Definition.has(PostgresEventStreamReader.class,
                        Definition.parameters(configuration)),
                EventStreamReader.class
        );

        gson = new Gson();
        setUpDatabase();

        insertEvent(1);
        insertEvent(2);
        insertEvent(3);
        insertEvent(4);
    }

    @After
    public void tearDown() throws Exception {
        dropDatabase();
        world.terminate();
    }

    @Test
    public void testThatCanReadAllEventsFromJournal() throws Exception {
        EventStream<AggregateRoot> stream = eventStreamReader.streamFor(streamName).await();
        assertEquals(State.NullState.Object, stream.snapshot);
        assertEquals(5, stream.streamVersion);
        assertEquals(stream.streamName, streamName);

        AtomicInteger eventNumber = new AtomicInteger(1);
        stream.events.forEach(event -> assertEquals(eventNumber.getAndIncrement(), event.eventData.number));
    }

    @Test
    public void testThatCanReadAllEventsFromJournalBasedOnOffset() throws Exception {
        AggregateRoot snapshotState = new AggregateRoot(streamName, 2);
        insertSnapshot(2, snapshotState);

        EventStream<AggregateRoot> stream = eventStreamReader.streamFor(streamName, 1).await();
        assertEquals(2, stream.snapshot.dataVersion);
        assertEquals(snapshotState, stream.snapshot.data);
        assertEquals(stream.streamVersion, 5);
        assertEquals(stream.streamName, streamName);

        Assert.assertEquals(2, stream.events.size());
        Assert.assertEquals(3, stream.events.get(0).eventData.number);
        Assert.assertEquals(4, stream.events.get(1).eventData.number);
    }

    private void setUpDatabase() throws SQLException {
        try (
                final PreparedStatement createEventTable = configuration.connection.prepareStatement(EVENT_TABLE);
                final PreparedStatement createSnapshotTable = configuration.connection.prepareStatement(SNAPSHOT_TABLE)
        ) {
            assert createEventTable.executeUpdate() == 0;
            assert createSnapshotTable.executeUpdate() == 0;
        }
    }

    private void dropDatabase() throws SQLException {
        try (
                final PreparedStatement dropEventTable = configuration.connection.prepareStatement(DROP_EVENT_TABLE);
                final PreparedStatement dropSnapshotTable = configuration.connection.prepareStatement(DROP_SNAPSHOT_TABLE)
        ) {
            assert dropEventTable.executeUpdate() == 0;
            assert dropSnapshotTable.executeUpdate() == 0;
        }
    }

    private void insertEvent(final int dataVersion) throws SQLException {
        try (final PreparedStatement stmt = configuration.connection.prepareStatement(INSERT_EVENT)) {
            stmt.setString(1, gson.toJson(new AggregateRoot(aggregateRootId, dataVersion)));
            stmt.setString(2, AggregateRoot.class.getCanonicalName());
            stmt.setString(3, streamName);

            assert stmt.executeUpdate() == 1;
            configuration.connection.commit();
        }
    }

    private void insertSnapshot(final int dataVersion, final AggregateRoot state) throws SQLException {
        try (final PreparedStatement stmt = configuration.connection.prepareStatement(INSERT_SNAPSHOT)) {
            stmt.setString(1, streamName);
            stmt.setString(2, AggregateRoot.class.getCanonicalName());
            stmt.setString(3, gson.toJson(state));
            stmt.setInt(4, dataVersion);

            assert stmt.executeUpdate() == 1;
            configuration.connection.commit();
        }
    }
}

class AggregateRoot {
    public final String id;
    public final int number;

    public AggregateRoot(String id, int number) {
        this.id = id;
        this.number = number;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AggregateRoot that = (AggregateRoot) o;
        return number == that.number &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, number);
    }

    @Override
    public String toString() {
        return "AggregateRoot{" +
                "id='" + id + '\'' +
                ", number=" + number +
                '}';
    }
}