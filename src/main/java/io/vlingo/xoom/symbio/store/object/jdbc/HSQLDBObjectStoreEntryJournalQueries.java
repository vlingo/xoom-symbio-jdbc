// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc;

import java.sql.Connection;
import java.text.MessageFormat;

/**
 * A {@code JDBCObjectStoreEntryJournalQueries} for HSQLDB.
 */
public class HSQLDBObjectStoreEntryJournalQueries extends JDBCObjectStoreEntryJournalQueries {

  /*
   * @see io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreEntryJournalQueries#upsertCurrentEntryOffsetQuery(java.lang.String[])
   */
  @Override
  public String upsertCurrentEntryOffsetQuery(final String[] placeholders) {
    return MessageFormat.format(
            "MERGE INTO {0} \n" +
            "USING (VALUES ?, ?) \n " +
            "O (O_READER_NAME, O_READER_OFFSET) \n" +
            "ON ({0}.O_READER_NAME = O.O_READER_NAME) \n" +
            "WHEN MATCHED THEN UPDATE \n" +
                    "SET {0}.O_READER_OFFSET = O.O_READER_OFFSET \n" +
            "WHEN NOT MATCHED THEN INSERT \n" +
                    "(O_READER_NAME, O_READER_OFFSET) \n" +
                    "VALUES (O.O_READER_NAME, O.O_READER_OFFSET)",
            EntryReaderOffsetsTableName);
  }

  /*
   * @see io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreEntryJournalQueries#wideTextDataType()
   */
  @Override
  public String wideTextDataType() {
    return "LONGVARCHAR(65535)";
  }
}
