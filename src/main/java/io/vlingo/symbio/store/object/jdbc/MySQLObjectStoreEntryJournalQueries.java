// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;

/**
 * A {@code JDBCObjectStoreEntryJournalQueries} for MySQL.
 */
public class MySQLObjectStoreEntryJournalQueries extends JDBCObjectStoreEntryJournalQueries {
    /**
     * Construct my state.
     * @param connection the Connection on which to perform queries
     */
    public MySQLObjectStoreEntryJournalQueries(Connection connection) {
        super(connection);
    }

    /**
     * @see JDBCObjectStoreEntryJournalQueries#createTextEntryJournalReaderOffsetsTable()
     */
    @Override
    public void createTextEntryJournalReaderOffsetsTable() throws SQLException {
        // VARCHAR(1024) => Error Code: 1071. Specified key was too long; max key length is 3072 bytes
        connection
                .createStatement()
                .execute("CREATE TABLE IF NOT EXISTS " + EntryReaderOffsetsTableName +
                        " (O_READER_NAME VARCHAR(512) PRIMARY KEY, O_READER_OFFSET BIGINT NOT NULL)");
    }

    /**
     * @see io.vlingo.symbio.store.object.jdbc.JDBCObjectStoreEntryJournalQueries#upsertCurrentEntryOffsetQuery(java.lang.String[])
     */
    @Override
    public String upsertCurrentEntryOffsetQuery(final String[] placeholders) {
        return MessageFormat.format(
                "INSERT INTO {0} (O_READER_NAME, O_READER_OFFSET) VALUES ({1}, {2}) " +
                        "ON DUPLICATE KEY UPDATE O_READER_OFFSET = {2}",
                EntryReaderOffsetsTableName,
                placeholders[0],
                placeholders[1]);
    }

    /*
     * @see io.vlingo.symbio.store.object.jdbc.JDBCObjectStoreEntryJournalQueries#wideTextDataType()
     */
    @Override
    public String wideTextDataType() {
        return "TEXT";
    }
}
