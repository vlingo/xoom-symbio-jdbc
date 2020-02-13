// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc.postgres;

public interface PostgresQueries {
    final static String SQL_STATE_READ =
            "SELECT tbl_{0}.s_type, tbl_{0}.s_type_version, tbl_{0}.s_data, tbl_{0}.s_data_version, tbl_{0}.s_metadata_value, tbl_{0}.s_metadata_op " +
                    "FROM tbl_{0} " +
                    "WHERE tbl_{0}.s_id = ?";

    final static String SQL_STATE_WRITE =
            "INSERT INTO tbl_{0} \n" +
                    "(s_id, s_type, s_type_version, s_data, s_data_version, s_metadata_value, s_metadata_op) \n" +
                    "VALUES (?, ?, ?, {1}, ?, ?, ?) \n" +
                    "ON CONFLICT (s_id) DO UPDATE SET \n" +
                    "s_type = EXCLUDED.s_type, \n" +
                    "s_type_version = EXCLUDED.s_type_version, \n" +
                    "s_data = EXCLUDED.s_data, \n" +
                    "s_data_version = EXCLUDED.s_data_version, \n" +
                    "s_metadata_value = EXCLUDED.s_metadata_value, \n" +
                    "s_metadata_op = EXCLUDED.s_metadata_op \n";

    final static String SQL_FORMAT_BINARY_CAST = "?";
    final static String SQL_FORMAT_TEXT_CAST = "?::JSON";

    final static String SQL_CREATE_STATE_STORE =
            "CREATE TABLE {0} (\n" +
                    "   s_id VARCHAR(128) NOT NULL,\n" +
                    "   s_type VARCHAR(256) NOT NULL,\n" +
                    "   s_type_version INT NOT NULL,\n" +
                    "   s_data {1} NOT NULL,\n" +
                    "   s_data_version INT NOT NULL,\n" +
                    "   s_metadata_value TEXT NOT NULL,\n" +
                    "   s_metadata_op VARCHAR(128) NOT NULL,\n" +
                    "   PRIMARY KEY (s_id) \n" +
                    ");";

    final static String SQL_FORMAT_BINARY = "bytea";
    final static String SQL_FORMAT_TEXT1 = "json";
    // private final static String SQL_FORMAT_TEXT2 = "jsonb";

    final static String TBL_VLINGO_SYMBIO_DISPATCHABLES = "tbl_vlingo_symbio_dispatchables";

    final static String SQL_CREATE_DISPATCHABLES_STORE =
            "CREATE TABLE {0} (\n" +
                    "   d_id BIGSERIAL PRIMARY KEY," +
                    "   d_created_at TIMESTAMP NOT NULL," +
                    "   d_originator_id VARCHAR(32) NOT NULL," +
                    "   d_dispatch_id VARCHAR(128) NOT NULL,\n" +
                    "   d_state_id VARCHAR(128) NOT NULL, \n" +
                    "   d_state_type VARCHAR(256) NOT NULL,\n" +
                    "   d_state_type_version INT NOT NULL,\n" +
                    "   d_state_data {1} NOT NULL,\n" +
                    "   d_state_data_version INT NOT NULL,\n" +
                    "   d_state_metadata_value TEXT NOT NULL,\n" +
                    "   d_state_metadata_op VARCHAR(128) NOT NULL,\n" +
                    "   d_state_metadata_object TEXT,\n" +
                    "   d_state_metadata_object_type VARCHAR(256),\n" +
                    "   d_entries TEXT\n" +
                    ");";

    final static String SQL_DISPATCH_ID_INDEX =
            "CREATE INDEX idx_dispatchables_dispatch_id \n" +
                    "ON {0} (d_dispatch_id);";

    final static String SQL_ORIGINATOR_ID_INDEX =
            "CREATE INDEX idx_dispatchables_originator_id \n" +
                    "ON {0} (d_originator_id);";

    final static String SQL_DISPATCHABLE_APPEND =
            "INSERT INTO {0} \n" +
                    "(d_id, d_created_at, d_originator_id, d_dispatch_id, \n" +
                    " d_state_id, d_state_type, d_state_type_version, \n" +
                    " d_state_data, d_state_data_version, \n" +
                    " d_state_metadata_value, d_state_metadata_op, d_state_metadata_object, d_state_metadata_object_type, d_entries) \n" +
                    "VALUES (DEFAULT, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    final static String SQL_DISPATCHABLE_DELETE =
            "DELETE FROM {0} WHERE d_dispatch_id = ?";

    final static String SQL_DISPATCHABLE_SELECT =
            "SELECT d_created_at, d_dispatch_id, d_state_id, d_state_type, d_state_type_version, d_state_data, d_state_data_version, \n" +
                    "       d_state_metadata_value, d_state_metadata_op, d_state_metadata_object, d_state_metadata_object_type, d_entries \n" +
                    "FROM {0} \n" +
                    "WHERE d_originator_id = ? ORDER BY d_created_at ASC";


    final static String SQL_CREATE_ENTRY_STORE =
            "CREATE TABLE {0} (\n" +
                    "   e_id BIGSERIAL PRIMARY KEY," +
                    "   e_type VARCHAR(256) NOT NULL,\n" +
                    "   e_type_version INT NOT NULL,\n" +
                    "   e_data {1} NOT NULL,\n" +
                    "   e_metadata_value VARCHAR(4000) NOT NULL,\n" +
                    "   e_metadata_op VARCHAR(128) NOT NULL\n" +
                    ");";

    final static String TBL_VLINGO_SYMBIO_STATE_ENTRY = "tbl_vlingo_symbio_state_entry";

    final static String SQL_CREATE_ENTRY_STORE_OFFSETS =
            "CREATE TABLE {0} (\n" +
                    "   reader_name VARCHAR(128) PRIMARY KEY," +
                    "   reader_offset BIGINT NOT NULL\n" +
                    ");";

    final static String TBL_VLINGO_SYMBIO_STATE_ENTRY_OFFSETS = "tbl_vlingo_symbio_state_entry_offsets";

    final static String SQL_APPEND_ENTRY =
            "INSERT INTO {0} \n" +
                    "(e_id, e_type, e_type_version, e_data, e_metadata_value, e_metadata_op) \n" +
                    "VALUES (DEFAULT, ?, ?, ?, ?, ?)";

    final static String SQL_APPEND_ENTRY_IDENTITY = "SELECT lastval()";

    final static String SQL_QUERY_ENTRY_BATCH =
            "SELECT e_id, e_type, e_type_version, e_data, e_metadata_value, e_metadata_op FROM " +
                    " {0} WHERE E_ID >= ? " +
                    "ORDER BY e_id LIMIT ?";

    final static String SQL_QUERY_ENTRY =
            "SELECT e_id, e_type, e_type_version, e_data, e_metadata_value, e_metadata_op FROM " +
                    " {0} WHERE e_id = ? ";

    final static String QUERY_LATEST_OFFSET =
            "SELECT reader_offset FROM {0} " +
                    "WHERE reader_name = ?";

    final static String QUERY_COUNT =
            "SELECT COUNT(*) FROM {0}";

    final static String UPDATE_CURRENT_OFFSET =
            "INSERT INTO {0}(reader_offset, reader_name) VALUES(?, ?) " +
                    "ON CONFLICT (reader_name) DO UPDATE SET reader_offset=?";

}
