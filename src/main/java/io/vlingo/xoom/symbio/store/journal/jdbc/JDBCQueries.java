// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import io.vlingo.xoom.common.Tuple2;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.journal.jdbc.mysql.MySQLQueries;
import io.vlingo.xoom.symbio.store.journal.jdbc.postgres.PostgresQueries;
import io.vlingo.xoom.symbio.store.journal.jdbc.postgres.yugabyte.YugaByteQueries;

public abstract class JDBCQueries {
    protected final Connection connection;

    protected final PreparedStatement deleteDispatchable;

    protected final PreparedStatement insertEntry;
    protected final PreparedStatement insertOffset;
    protected final PreparedStatement insertSnapshot;
    protected final PreparedStatement insertDispatchable;

    protected final PreparedStatement selectCurrentOffset;
    protected final PreparedStatement selectDispatchables;
    protected final PreparedStatement selectLastOffset;
    protected final PreparedStatement selectJournalCount;
    protected final PreparedStatement selectEntry;
    protected final PreparedStatement selectEntryBatch;
    protected final PreparedStatement selectSnapshot;
    protected final PreparedStatement selectStream;

    protected final PreparedStatement updateOffset;
    protected final PreparedStatement upsertOffset;

    public JDBCQueries(final Connection connection) throws SQLException {
        this.connection = connection;

        this.deleteDispatchable = connection.prepareStatement(deleteDispatchableQuery());

        this.insertEntry = connection.prepareStatement(insertEntryQuery(), generatedKeysIndicator());
        this.insertOffset = connection.prepareStatement(insertOffsetQuery());
        this.insertSnapshot = connection.prepareStatement(insertSnapshotQuery());
        this.insertDispatchable = connection.prepareStatement(insertDispatchableQuery());

        this.selectCurrentOffset = connection.prepareStatement(selectCurrentOffset());
        this.selectDispatchables = connection.prepareStatement(selectDispatchablesQuery());
        this.selectEntry = connection.prepareStatement(selectEntryQuery());
        this.selectEntryBatch = connection.prepareStatement(selectEntryBatchQuery());
        this.selectLastOffset = connection.prepareStatement(selectLastOffsetQuery());
        this.selectJournalCount = connection.prepareStatement(selectJournalCountQuery());
        this.selectSnapshot = connection.prepareStatement(selectSnapshotQuery());
        this.selectStream = connection.prepareStatement(selectStreamQuery());

        this.updateOffset = connection.prepareStatement(updateOffsetQuery());
        this.upsertOffset = connection.prepareStatement(upsertOffsetQuery());
    }

    /**
     * Answer a new {@code PostgresQueries} per the {@code DatabaseType} of the {@code connection}.
     * @param connection the Connection to use
     * @return PostgresQueries
     * @throws SQLException if the specific PostgresQueries cannot be created
     */
    public static JDBCQueries queriesFor(final Connection connection) throws SQLException {
        final DatabaseType databaseType = DatabaseType.databaseType(connection);

        switch (databaseType) {
            case Postgres:
                return new PostgresQueries(connection);
            case YugaByte:
                return new YugaByteQueries(connection);
            case MySQL:
                return new MySQLQueries(connection);
            default:
                throw new IllegalArgumentException("Database type not supported: " + databaseType);
        }
    }

    public void close() throws SQLException {
        close(deleteDispatchable);
        close(insertEntry);
        close(insertOffset);
        close(insertSnapshot);
        close(insertDispatchable);
        close(selectCurrentOffset);
        close(selectDispatchables);
        close(selectEntry);
        close(selectEntryBatch);
        close(selectLastOffset);
        close(selectJournalCount);
        close(selectSnapshot);
        close(selectStream);
        close(updateOffset);
        close(upsertOffset);

        connection.close();
    }

    public void createTables() throws SQLException {
        connection.createStatement().execute(createJournalTableQuery());
        connection.commit();
        connection.createStatement().execute(createOffsetsTable());
        connection.commit();
        connection.createStatement().execute(createSnapshotsTableQuery());
        connection.commit();
        connection.createStatement().execute(createDispatchableTable());
        connection.commit();
    }

    public void dropTables() throws SQLException {
        connection.prepareStatement(dropDispatchablesTableQuery()).execute();
        connection.commit();
        connection.prepareStatement(dropSnapshotsTableQuery()).execute();
        connection.commit();
        connection.prepareStatement(dropOffsetsTable()).execute();
        connection.commit();
        connection.prepareStatement(dropJournalTable()).execute();
        connection.commit();
    }

    public PreparedStatement prepareDeleteDispatchableQuery(
            final String dispatchableId)
            throws SQLException {

        deleteDispatchable.clearParameters();

        deleteDispatchable.setString(1, dispatchableId);

        return deleteDispatchable;
    }

    public long generatedKeyFrom(PreparedStatement insertStatement) throws SQLException {
        try (final ResultSet result = insertStatement.getGeneratedKeys()) {
            if (result.next()) {
                return result.getLong(1);
            }
            return -1L;
        }
    }

    public Tuple2<PreparedStatement,Optional<String>> prepareInsertDispatchableQuery(
            final String d_dispatch_id,
            final String d_originator_id,
            final String d_state_id,
            final String d_state_data,
            final int d_state_data_version,
            final String d_state_type,
            final int d_state_type_version,
            final String d_state_metadata,
            final String d_entries)
            throws SQLException {

        insertDispatchable.clearParameters();

        insertDispatchable.setString(1, d_dispatch_id);
        insertDispatchable.setString(2, d_originator_id);
        insertDispatchable.setLong(3, System.currentTimeMillis());

        insertDispatchable.setString(4, d_state_id);
        insertDispatchable.setString(5, d_state_data);
        insertDispatchable.setInt(6, d_state_data_version);
        insertDispatchable.setString(7, d_state_type);
        insertDispatchable.setInt(8, d_state_type_version);
        insertDispatchable.setString(9, d_state_metadata);
        insertDispatchable.setString(10, d_entries);

        return Tuple2.from(insertDispatchable, Optional.empty());
    }

    public Tuple2<PreparedStatement,Optional<String>> prepareInsertEntryQuery(
            final String stream_name,
            final int stream_version,
            final String entry_data,
            final String entry_type,
            final int entry_type_version,
            final String entry_metadata)
            throws SQLException {

        insertEntry.clearParameters();

        insertEntry.setString(1, stream_name);
        insertEntry.setInt(2, stream_version);

        insertEntry.setString(3, entry_data);
        insertEntry.setString(4, entry_type);
        insertEntry.setInt(5, entry_type_version);

        insertEntry.setString(6, entry_metadata);

        return Tuple2.from(insertEntry, Optional.empty());
    }

    public PreparedStatement prepareInsertOffsetQuery(
            final String readerName,
            final long readerOffset)
            throws SQLException {

        insertOffset.clearParameters();

        insertOffset.setString(1, readerName);
        insertOffset.setLong(2, readerOffset);

        return insertOffset;
    }

    public Tuple2<PreparedStatement,Optional<String>> prepareInsertSnapshotQuery(
            final String stream_name,
            final int stream_version,
            final String e_snapshot_data,
            final int e_snapshot_data_version,
            final String e_snapshot_type,
            final int e_snapshot_type_version,
            final String e_snapshot_metadata)
            throws SQLException {

        insertSnapshot.clearParameters();

        insertSnapshot.setString(1, stream_name);
        insertSnapshot.setInt(2, stream_version);

        insertSnapshot.setString(3, e_snapshot_data);
        insertSnapshot.setInt(4, e_snapshot_data_version);

        insertSnapshot.setString(5, e_snapshot_type);
        insertSnapshot.setInt(6, e_snapshot_type_version);

        insertSnapshot.setString(7, e_snapshot_metadata);

        return Tuple2.from(insertSnapshot, Optional.empty());
    }

    public PreparedStatement prepareSelectCurrentOffsetQuery(
            final String readerName)
            throws SQLException {

        selectCurrentOffset.clearParameters();

        selectCurrentOffset.setString(1, readerName);

        return selectCurrentOffset;
    }

    public PreparedStatement prepareSelectDispatchablesQuery(
            final String oringinatorId)
            throws SQLException {

        selectDispatchables.clearParameters();

        selectDispatchables.setString(1, oringinatorId);

        return selectDispatchables;
    }

    public PreparedStatement prepareSelectEntryQuery(
            final long entryId)
            throws SQLException {

        selectEntry.clearParameters();

        selectEntry.setLong(1, entryId);

        return selectEntry;
    }

    public PreparedStatement prepareSelectEntryBatchQuery(
            final long entryId,
            final int count)
            throws SQLException {

        selectEntryBatch.clearParameters();

        selectEntryBatch.setLong(1, entryId);
        selectEntryBatch.setLong(2, entryId + count - 1);

        return selectEntryBatch;
    }

    /**
     * Prepare always a new {@link PreparedStatement} which contains SELECT query of entries based on ids.
     * @param ids the {@code List<Long>} of identities to use in the query
     * @return a {@link PreparedStatement} which needs to be closed due to variable size of ids.
     * @throws SQLException if the statement creation fails
     */
    public PreparedStatement prepareNewSelectEntriesByIdsQuery(List<Long> ids) throws SQLException {
        String[] placeholderList = new String[ids.size()];
        Arrays.fill(placeholderList, "?");
        String placeholders = String.join(", ", placeholderList);
        String query = MessageFormat.format(selectEntriesByIds(), placeholders);
        PreparedStatement preparedStatement = connection.prepareStatement(query);

        for (int i = 0; i < ids.size(); i++) {
            preparedStatement.setLong(i + 1, ids.get(i));
        }

        return preparedStatement;
    }

    public PreparedStatement prepareSelectLastOffsetQuery() {
        return selectLastOffset;
    }

    public PreparedStatement prepareSelectJournalCount() {
        return selectJournalCount;
    }

    public PreparedStatement prepareSelectSnapshotQuery(
            final String streamName)
            throws SQLException {

        selectSnapshot.clearParameters();

        selectSnapshot.setString(1, streamName);

        return selectSnapshot;
    }

    public PreparedStatement prepareSelectStreamQuery(
            final String streamName,
            final int streamVersion)
            throws SQLException {

        selectStream.clearParameters();

        selectStream.setString(1, streamName);
        selectStream.setInt(2, streamVersion);

        return selectStream;
    }

    public PreparedStatement prepareUpdateOffsetQuery(
            final String readerName,
            final long readerOffset)
            throws SQLException {

        updateOffset.clearParameters();

        updateOffset.setLong(1, readerOffset);
        updateOffset.setString(2, readerName);

        return updateOffset;
    }

    public PreparedStatement prepareUpsertOffsetQuery(
            final String readerName,
            final long readerOffset)
            throws SQLException {

        upsertOffset.clearParameters();

        upsertOffset.setString(1, readerName);
        upsertOffset.setLong(2, readerOffset);
        upsertOffset.setLong(3, readerOffset);

        return upsertOffset;
    }

    private void close(final PreparedStatement statement) {
        try {
            statement.close();
        } catch (Exception e) {
            // ignore
        }
    }

    protected abstract String createDispatchableTable();

    protected abstract String createJournalTableQuery();

    protected abstract String createOffsetsTable();

    protected abstract String createSnapshotsTableQuery();

    protected abstract String deleteDispatchableQuery();

    protected abstract String dropDispatchablesTableQuery();

    protected abstract String dropJournalTable();

    protected abstract String dropOffsetsTable();

    protected abstract String dropSnapshotsTableQuery();

    protected abstract int generatedKeysIndicator();

    protected abstract String insertDispatchableQuery();

    protected abstract String insertEntryQuery();

    protected abstract String insertOffsetQuery();

    protected abstract String insertSnapshotQuery();

    protected abstract String selectCurrentOffset();

    protected abstract String selectDispatchablesQuery();

    protected abstract String selectEntryQuery();

    protected abstract String selectEntryBatchQuery();

    protected abstract String selectEntriesByIds();

    protected abstract String selectLastOffsetQuery();

    protected abstract String selectJournalCountQuery();

    protected abstract String selectSnapshotQuery();

    protected abstract String selectStreamQuery();

    protected abstract String updateOffsetQuery();

    protected abstract String upsertOffsetQuery();
}
