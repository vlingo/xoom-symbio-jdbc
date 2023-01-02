// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.journal.jdbc.postgres.yugabyte;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;
import java.util.UUID;

import io.vlingo.xoom.common.Tuple2;
import io.vlingo.xoom.common.identity.IdentityGenerator;
import io.vlingo.xoom.symbio.store.journal.jdbc.postgres.PostgresQueries;

public class YugaByteQueries extends PostgresQueries {
  private static final String INSERT_ENTRY =
          "INSERT INTO xoom_symbio_journal " +
                  "(e_id, e_timestamp, e_stream_name, e_stream_version," +
                  " e_entry_data, e_entry_type, e_entry_type_version, e_entry_metadata) " +
          "VALUES(?, ?, ?, ?, ?, ?, ?, ?)";

  private final IdentityGenerator identityGenerator = new IdentityGenerator.TimeBasedIdentityGenerator();

  @Override
  public Tuple2<PreparedStatement, Optional<String>> prepareInsertEntryQuery(
      final Connection connection,
      final String stream_name,
      final int stream_version,
      final String entry_data,
      final String entry_type,
      final int entry_type_version,
      final String entry_metadata)
      throws SQLException {
    PreparedStatement insertEntry = newInsertEntryStatementQuery(connection);
    insertEntry.clearParameters();

    final UUID e_id = identityGenerator.generate();
    final long e_timestamp = e_id.timestamp();

    insertEntry.setObject(1, e_id);
    insertEntry.setLong(2, e_timestamp);

    insertEntry.setString(3, stream_name);
    insertEntry.setInt(4, stream_version);

    insertEntry.setString(5, entry_data);
    insertEntry.setString(6, entry_type);
    insertEntry.setInt(7, entry_type_version);

    insertEntry.setString(8, entry_metadata);

    return Tuple2.from(insertEntry, Optional.of(e_id.toString()));
  }

  @Override
  protected int generatedKeysIndicator() {
    return Statement.NO_GENERATED_KEYS;
  }

  @Override
  protected String insertEntryQuery() {
    return INSERT_ENTRY;
  }
}
