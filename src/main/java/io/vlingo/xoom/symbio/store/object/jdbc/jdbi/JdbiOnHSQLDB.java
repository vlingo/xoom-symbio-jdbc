// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jdbi;

import java.text.MessageFormat;
import java.util.function.BiFunction;

import org.jdbi.v3.core.statement.Update;

import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreEntryJournalQueries;

/**
 * The {@code JdbiOnDatabase} for HSQLDB.
 */
public class JdbiOnHSQLDB extends JdbiOnDatabase {
  /**
   * Answer a new {@code JdbiOnHSQLDB} using the {@code Configuration}.
   * @param configuration the Configuration which include the Connection
   * @return JdbiOnHSQLDB
   */
  public static JdbiOnHSQLDB openUsing(final Configuration configuration) {
    return new JdbiOnHSQLDB(configuration);
  }

  /**
   * @see io.vlingo.xoom.symbio.store.object.jdbc.jdbi.JdbiOnDatabase#currentEntryOffsetMapper(java.lang.String[])
   */
  @Override
  public JdbiPersistMapper currentEntryOffsetMapper(final String[] placeholders) {
    final String table = JDBCObjectStoreEntryJournalQueries.EntryReaderOffsetsTableName;

    return JdbiPersistMapper.with(
            MessageFormat.format(
                    "MERGE INTO " + table + "\n" +
                    "USING (VALUES {0}, {1}) \n " +
                    "O (O_READER_NAME, O_READER_OFFSET) \n" +
                    "ON (" + table + ".O_READER_NAME = O.O_READER_NAME) \n" +
                    "WHEN MATCHED THEN UPDATE \n" +
                            "SET " + table + ".O_READER_OFFSET = O.O_READER_OFFSET \n" +
                    "WHEN NOT MATCHED THEN INSERT \n" +
                            "(O_READER_NAME, O_READER_OFFSET) \n" +
                            "VALUES (O.O_READER_NAME, O.O_READER_OFFSET)",
                    placeholders[0],
                    placeholders[1]),
            null,
            (BiFunction<Update,Object,Update>[]) null);
  }

  private JdbiOnHSQLDB(final Configuration configuration) {
    super(configuration);
  }
}
