// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc.mysql;

import java.sql.Statement;

import io.vlingo.xoom.symbio.store.journal.jdbc.JDBCQueries;

public class MySQLQueries extends JDBCQueries {
    public static final String TABLE_VLINGO_SYMBIO_JOURNAL_DISPATCHABLES = "xoom_symbio_journal_dispatchables";
    public static final String TABLE_VLINGO_SYMBIO_JOURNAL = "xoom_symbio_journal";
    public static final String TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS = "xoom_symbio_journal_offsets";
    public static final String TABLE_VLINGO_SYMBIO_JOURNAL_SNAPSHOTS = "xoom_symbio_journal_snapshots";

    private static final String CREATE_DISPATCHABLE_TABLE =
            "CREATE TABLE IF NOT EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL_DISPATCHABLES + " (\n" +
                    "   D_DISPATCH_ID VARCHAR(512) PRIMARY KEY,\n" +
                    "   D_ORIGINATOR_ID VARCHAR(512) NOT NULL," +
                    "   D_CREATED_ON BIGINT NOT NULL," +
                    "   D_STATE_ID VARCHAR(512) NULL, \n" +
                    "   D_STATE_DATA TEXT NULL,\n" +
                    "   D_STATE_DATA_VERSION INT NULL,\n" +
                    "   D_STATE_TYPE VARCHAR(512) NULL,\n" +
                    "   D_STATE_TYPE_VERSION INTEGER NULL,\n" +
                    "   D_STATE_METADATA TEXT NULL,\n" +
                    "   D_ENTRIES TEXT NOT NULL\n" +
                    ");";

    private static final String CREATE_JOURNAL_TABLE =
            "CREATE TABLE IF NOT EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL + " (\n" +
//                  "E_ID BIGINT GENERATED ALWAYS AS IDENTITY(START WITH 1 INCREMENT BY 1) PRIMARY KEY, \n" +
                    "E_ID SERIAL PRIMARY KEY, \n" +
                    "E_STREAM_NAME VARCHAR(512) NOT NULL, \n" +
                    "E_STREAM_VERSION INTEGER NOT NULL, \n" +
                    "E_ENTRY_DATA TEXT NOT NULL, \n" +
                    "E_ENTRY_TYPE VARCHAR(512) NOT NULL, \n" +
                    "E_ENTRY_TYPE_VERSION INTEGER NOT NULL, \n" +
                    "E_ENTRY_METADATA TEXT NOT NULL \n" +
                    ")";

    private static final String CREATE_OFFSETS_TABLE =
            "CREATE TABLE IF NOT EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS + "(" +
                    "O_READER_NAME VARCHAR(128) PRIMARY KEY," +
                    "O_READER_OFFSET BIGINT NOT NULL" +
                    ")";

    private static final String CREATE_SNAPSHOTS_TABLE =
            "CREATE TABLE IF NOT EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL_SNAPSHOTS + " (\n" +
                    "S_STREAM_NAME VARCHAR(512) NOT NULL, \n" +
                    "S_STREAM_VERSION INTEGER NOT NULL, \n" +
                    "S_SNAPSHOT_DATA TEXT NOT NULL, \n" +
                    "S_SNAPSHOT_DATA_VERSION INTEGER NOT NULL, \n" +
                    "S_SNAPSHOT_TYPE VARCHAR(512) NOT NULL, \n" +
                    "S_SNAPSHOT_TYPE_VERSION INTEGER NOT NULL, \n" +
                    "S_SNAPSHOT_METADATA TEXT NOT NULL, \n\n" +

                    "PRIMARY KEY (S_STREAM_NAME, S_STREAM_VERSION) \n" +
                    ")";

    private final static String DELETE_DISPATCHABLE =
            "DELETE FROM " + TABLE_VLINGO_SYMBIO_JOURNAL_DISPATCHABLES + " " +
                    "WHERE D_DISPATCH_ID = ?";

    private static final String DROP_DISPATCHABLES_TABLE =
            "DROP TABLE IF EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL_DISPATCHABLES;

    private static final String DROP_JOURNAL_TABLE =
            "DROP TABLE IF EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL;

    private static final String DROP_OFFSETS_TABLE =
            "DROP TABLE IF EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS;

    private static final String DROP_SNAPSHOTS_TABLE =
            "DROP TABLE IF EXISTS " + TABLE_VLINGO_SYMBIO_JOURNAL_SNAPSHOTS;

    private final static String INSERT_DISPATCHABLE =
            "INSERT INTO " + TABLE_VLINGO_SYMBIO_JOURNAL_DISPATCHABLES + " \n" +
                    "(D_DISPATCH_ID, D_ORIGINATOR_ID, D_CREATED_ON, \n" +
                    " D_STATE_ID, D_STATE_DATA, D_STATE_DATA_VERSION, \n" +
                    " D_STATE_TYPE, D_STATE_TYPE_VERSION, \n" +
                    " D_STATE_METADATA, D_ENTRIES) \n" +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private static final String INSERT_ENTRY =
            "INSERT INTO " + TABLE_VLINGO_SYMBIO_JOURNAL + " \n" +
                    "(E_STREAM_NAME, E_STREAM_VERSION, E_ENTRY_DATA, \n" +
                    " E_ENTRY_TYPE, E_ENTRY_TYPE_VERSION, E_ENTRY_METADATA) \n" +
                    "VALUES(?, ?, ?, ?, ?, ?)";

    private static final String INSERT_OFFSET =
            "INSERT INTO " + TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS + " (O_READER_NAME, O_READER_OFFSET) VALUES(?, ?)";

    private static final String UPDATE_OFFSET =
            "UPDATE  " + TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS + " SET O_READER_OFFSET = ? WHERE O_READER_NAME = ?";

    private static final String UPSERT_OFFSET =
            "INSERT INTO " + TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS + "(O_READER_NAME, O_READER_OFFSET) VALUES(?, ?) " +
                    "ON DUPLICATE KEY UPDATE O_READER_OFFSET = ?";

    private static final String INSERT_SNAPSHOT =
            "INSERT INTO " + TABLE_VLINGO_SYMBIO_JOURNAL_SNAPSHOTS + "\n" +
                    "(S_STREAM_NAME, S_STREAM_VERSION, \n" +
                    " S_SNAPSHOT_DATA, S_SNAPSHOT_DATA_VERSION, \n" +
                    " S_SNAPSHOT_TYPE, S_SNAPSHOT_TYPE_VERSION, \n" +
                    " S_SNAPSHOT_METADATA) \n" +
                    "VALUES(?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_CURRENT_OFFSET =
            "SELECT O_READER_OFFSET FROM " + TABLE_VLINGO_SYMBIO_JOURNAL_OFFSETS + " WHERE O_READER_NAME=?";

    private final static String SELECT_DISPATCHABLES =
            "SELECT D_DISPATCH_ID, D_CREATED_ON, \n" +
                    " D_STATE_ID, D_STATE_DATA, D_STATE_DATA_VERSION, \n" +
                    " D_STATE_TYPE, D_STATE_TYPE_VERSION, \n" +
                    " D_STATE_METADATA, D_ENTRIES \n" +
                    " FROM " + TABLE_VLINGO_SYMBIO_JOURNAL_DISPATCHABLES + "\n" +
                    " WHERE D_ORIGINATOR_ID = ? ORDER BY D_CREATED_ON";

    private static final String SELECT_ENTRY =
            "SELECT E_ID, E_ENTRY_DATA, E_ENTRY_TYPE, E_ENTRY_TYPE_VERSION, E_ENTRY_METADATA, E_STREAM_VERSION " +
                    "FROM " + TABLE_VLINGO_SYMBIO_JOURNAL + " " +
                    "WHERE E_ID = ?";

    private static final String SELECT_ENTRY_BATCH =
            "SELECT E_ID, E_ENTRY_DATA, E_ENTRY_TYPE, E_ENTRY_TYPE_VERSION, E_ENTRY_METADATA, E_STREAM_VERSION " +
                    "FROM " + TABLE_VLINGO_SYMBIO_JOURNAL + " " +
                    "WHERE E_ID BETWEEN ? AND ? ORDER BY E_ID";

    private static final String SELECT_ENTRY_IDS =
            "SELECT E_ID, E_ENTRY_DATA, E_ENTRY_TYPE, E_ENTRY_TYPE_VERSION, E_ENTRY_METADATA, E_STREAM_VERSION " +
                    "FROM " + TABLE_VLINGO_SYMBIO_JOURNAL + " " +
                    "WHERE E_ID IN ({0}) ORDER BY E_ID";

    private static final String SELECT_LAST_OFFSET =
            "SELECT MAX(E_ID) FROM " + TABLE_VLINGO_SYMBIO_JOURNAL;

    private static final String SELECT_JOURNAL_COUNT =
            "SELECT COUNT(*) FROM " + TABLE_VLINGO_SYMBIO_JOURNAL;

    private static final String SELECT_SNAPSHOT =
            "SELECT S_SNAPSHOT_DATA, S_SNAPSHOT_DATA_VERSION, S_SNAPSHOT_TYPE, S_SNAPSHOT_TYPE_VERSION, S_SNAPSHOT_METADATA " +
                    "FROM " + TABLE_VLINGO_SYMBIO_JOURNAL_SNAPSHOTS + " WHERE S_STREAM_NAME = ?";

    private static final String SELECT_STREAM =
            "SELECT E_ID, E_STREAM_VERSION, E_ENTRY_DATA, E_ENTRY_TYPE, E_ENTRY_TYPE_VERSION, E_ENTRY_METADATA " +
                    "FROM " + TABLE_VLINGO_SYMBIO_JOURNAL + " " +
                    "WHERE E_STREAM_NAME = ? AND E_STREAM_VERSION >= ? ORDER BY E_STREAM_VERSION";

    @Override
    protected String createDispatchableTable() {
        return CREATE_DISPATCHABLE_TABLE;
    }

    @Override
    protected String createJournalTableQuery() {
        return CREATE_JOURNAL_TABLE;
    }

    @Override
    protected String createOffsetsTable() {
        return CREATE_OFFSETS_TABLE;
    }

    @Override
    protected String createSnapshotsTableQuery() {
        return CREATE_SNAPSHOTS_TABLE;
    }

    @Override
    protected String deleteDispatchableQuery() {
        return DELETE_DISPATCHABLE;
    }

    @Override
    protected String dropDispatchablesTableQuery() {
        return DROP_DISPATCHABLES_TABLE;
    }

    @Override
    protected String dropJournalTable() {
        return DROP_JOURNAL_TABLE;
    }

    @Override
    protected String dropOffsetsTable() {
        return DROP_OFFSETS_TABLE;
    }

    @Override
    protected String dropSnapshotsTableQuery() {
        return DROP_SNAPSHOTS_TABLE;
    }

    @Override
    protected int generatedKeysIndicator() {
        return Statement.RETURN_GENERATED_KEYS;
    }

    @Override
    protected String insertDispatchableQuery() {
        return INSERT_DISPATCHABLE;
    }

    @Override
    protected String insertEntryQuery() {
        return INSERT_ENTRY;
    }

    @Override
    protected String insertOffsetQuery() {
        return INSERT_OFFSET;
    }

    @Override
    protected String insertSnapshotQuery() {
        return INSERT_SNAPSHOT;
    }

    @Override
    protected String selectCurrentOffset() {
        return SELECT_CURRENT_OFFSET;
    }

    @Override
    protected String selectDispatchablesQuery() {
        return SELECT_DISPATCHABLES;
    }

    @Override
    protected String selectEntryQuery() {
        return SELECT_ENTRY;
    }

    @Override
    protected String selectEntryBatchQuery() {
        return SELECT_ENTRY_BATCH;
    }

    @Override
    protected String selectEntriesByIds() {
        return SELECT_ENTRY_IDS;
    }

    @Override
    protected String selectLastOffsetQuery() {
        return SELECT_LAST_OFFSET;
    }

    @Override
    protected String selectJournalCountQuery() {
        return SELECT_JOURNAL_COUNT;
    }

    @Override
    protected String selectSnapshotQuery() {
        return SELECT_SNAPSHOT;
    }

    @Override
    protected String selectStreamQuery() {
        return SELECT_STREAM;
    }

    @Override
    protected String updateOffsetQuery() {
        return UPDATE_OFFSET;
    }

    @Override
    protected String upsertOffsetQuery() {
        return UPSERT_OFFSET;
    }
}
