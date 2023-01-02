// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jdbi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;

import io.vlingo.xoom.symbio.store.StoredTypes;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import com.google.gson.reflect.TypeToken;

import io.vlingo.xoom.common.serialization.JsonSerialization;
import io.vlingo.xoom.symbio.BaseEntry.TextEntry;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;

/**
 * A {@code RowMapper} for text-based {@code Dispatchable<Entry<?>, State<?>} instances.
 */
public class TextDispatchablesMapper implements RowMapper<Dispatchable<Entry<?>, State<?>>> {
  @Override
  public Dispatchable<Entry<?>, State<?>> map(final ResultSet rs, final StatementContext ctx) throws SQLException {
    final String dispatchId = rs.getString("D_DISPATCH_ID");
    final LocalDateTime createdAt = rs.getTimestamp("D_CREATED_AT").toLocalDateTime();
    final Class<?> stateType;
    try {
      stateType = StoredTypes.forName(rs.getString("D_STATE_TYPE"));
    } catch (final ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
    final int stateTypeVersion = rs.getInt("D_STATE_TYPE_VERSION");
    final int stateDataVersion = rs.getInt("D_STATE_DATA_VERSION");
    final String stateId = rs.getString("D_STATE_ID");
    final String stateData = rs.getString("D_STATE_DATA");
    final String metadataValue = rs.getString("D_STATE_METADATA");
    final Metadata metadata = JsonSerialization.deserialized(metadataValue, Metadata.class);

    final String entriesJson = rs.getString("D_ENTRIES");
    final List<Entry<?>> entries;
    if (entriesJson !=null && !entriesJson.isEmpty()){
      entries = JsonSerialization.deserializedList(entriesJson, new TypeToken<List<TextEntry>>(){}.getType());
    } else {
      entries = Collections.emptyList();
    }

    final State.TextState state = new State.TextState(stateId, stateType, stateTypeVersion, stateData, stateDataVersion, metadata);
    return new Dispatchable<>(dispatchId, createdAt, state, entries);
  }
}
