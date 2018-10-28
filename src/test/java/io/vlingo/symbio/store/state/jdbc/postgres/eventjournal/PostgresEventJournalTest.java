package io.vlingo.symbio.store.state.jdbc.postgres.eventjournal;

import com.google.gson.Gson;
import io.vlingo.actors.World;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.jdbc.Configuration;
import org.junit.After;
import org.junit.Before;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

import static io.vlingo.symbio.store.state.jdbc.postgres.PostgresConfigurationProvider.testConfiguration;

public abstract class PostgresEventJournalTest {
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

    protected Configuration configuration;
    protected World world;
    protected String aggregateRootId;
    protected Gson gson;
    protected String streamName;

    @Before
    @SuppressWarnings("unchecked")
    public void setUpDatabase() throws Exception {
        aggregateRootId = UUID.randomUUID().toString();
        streamName = aggregateRootId;
        world = World.startWithDefaults("event-stream-tests");
        configuration = testConfiguration(StateStore.DataFormat.Text);
        gson = new Gson();

        createDatabase();
    }

    @After
    public void tearDownDatabase() throws Exception {
        dropDatabase();
        world.terminate();
    }

    private void createDatabase() throws SQLException {
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

    protected final void insertEvent(final int dataVersion) throws SQLException {
        try (final PreparedStatement stmt = configuration.connection.prepareStatement(INSERT_EVENT)) {
            stmt.setString(1, gson.toJson(new TestAggregateRoot(aggregateRootId, dataVersion)));
            stmt.setString(2, TestAggregateRoot.class.getCanonicalName());
            stmt.setString(3, streamName);

            assert stmt.executeUpdate() == 1;
            configuration.connection.commit();
        }
    }

    protected final void insertSnapshot(final int dataVersion, final TestAggregateRoot state) throws SQLException {
        try (final PreparedStatement stmt = configuration.connection.prepareStatement(INSERT_SNAPSHOT)) {
            stmt.setString(1, streamName);
            stmt.setString(2, TestAggregateRoot.class.getCanonicalName());
            stmt.setString(3, gson.toJson(state));
            stmt.setInt(4, dataVersion);

            assert stmt.executeUpdate() == 1;
            configuration.connection.commit();
        }
    }
}
