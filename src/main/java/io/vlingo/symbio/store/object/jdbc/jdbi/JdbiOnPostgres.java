// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import java.text.MessageFormat;

import io.vlingo.symbio.store.common.jdbc.Configuration;

/**
 * A {@code JdbiOnDatabase} for HSQLDB.
 */
public class JdbiOnPostgres extends JdbiOnDatabase {
  /**
   * Answer a new {@code JdbiOnHSQLDB} using the {@code Configuration}.
   * @param configuration the Configuration which include the Connection
   * @return JdbiOnHSQLDB
   */
  public static JdbiOnPostgres openUsing(final Configuration configuration) {
    return new JdbiOnPostgres(configuration);
  }

  /*
   * @see io.vlingo.symbio.store.object.jdbc.jdbi.JdbiOnDatabase#currentEntryOffsetMapper(java.lang.String[])
   */
  @Override
  public JdbiPersistMapper currentEntryOffsetMapper(final String[] placeholders) {
    return JdbiPersistMapper.with(
            MessageFormat.format(
                    "INSERT INTO TBL_VLINGO_OBJECTSTORE_ENTRYREADER_OFFSETS(O_READER_NAME, O_READER_OFFSET) VALUES({0}, {1}) " +
                            "ON CONFLICT (O_READER_NAME) DO UPDATE SET O_READER_OFFSET=?",
                    placeholders[0],
                    placeholders[1]),
            null,
            null);
  }

  private JdbiOnPostgres(final Configuration configuration) {
    super(configuration);
  }
}
