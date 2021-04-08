// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jdbi;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import io.vlingo.xoom.symbio.BaseEntry.TextEntry;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.store.object.PersistentEntry;

/**
 * A {@code RowMapper} for {@code TextEntry} instances.
 */
@SuppressWarnings("rawtypes")
public class TextEntryMapper implements RowMapper<Entry> {
  @Override
  public Entry map(final ResultSet rs, final StatementContext ctx) throws SQLException {
    final TextEntry entry =
            new TextEntry(
                    Long.toString(rs.getLong("E_ID")),
                    Entry.typed(rs.getString("E_TYPE")),
                    rs.getInt("E_TYPE_VERSION"),
                    rs.getString("E_DATA"),
                    rs.getInt("E_ENTRY_VERSION"),
                    Metadata.with(rs.getString("E_METADATA_VALUE"), rs.getString("E_METADATA_OP")));

    return new PersistentEntry(entry);
  }
}
