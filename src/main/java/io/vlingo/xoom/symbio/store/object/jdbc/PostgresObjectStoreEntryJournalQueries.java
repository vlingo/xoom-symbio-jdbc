// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc;

import java.text.MessageFormat;

/**
 * A {@code JDBCObjectStoreEntryJournalQueries} for Postgres.
 */
public class PostgresObjectStoreEntryJournalQueries extends JDBCObjectStoreEntryJournalQueries {

    /*
     * @see io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreEntryJournalQueries#upsertCurrentEntryOffsetQuery(java.lang.String[])
     */
    @Override
    public String upsertCurrentEntryOffsetQuery(final String[] placeholders) {
        return MessageFormat.format(
                "INSERT INTO {0}(O_READER_NAME, O_READER_OFFSET) VALUES({1}, {2}) " +
                        "ON CONFLICT (O_READER_NAME) DO UPDATE SET O_READER_OFFSET={2}",
                EntryReaderOffsetsTableName,
                placeholders[0],
                placeholders[1]);
    }

    /*
     * @see io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreEntryJournalQueries#wideTextDataType()
     */
    @Override
    public String wideTextDataType() {
        return "TEXT";
    }
}
