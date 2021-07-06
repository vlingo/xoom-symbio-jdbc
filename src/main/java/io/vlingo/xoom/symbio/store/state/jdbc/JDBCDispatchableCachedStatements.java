// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.CachedStatement;

public abstract class JDBCDispatchableCachedStatements<T> {
  private final String originatorId;
  @SuppressWarnings("unused")
  private final DataFormat format;
  private final T appendDataObject;
  private final Logger logger;

  protected JDBCDispatchableCachedStatements(
          final String originatorId,
          final DataFormat format,
          final T appendDataObject,
          final Logger logger) {
    this.originatorId = originatorId;
    this.format = format;
    this.appendDataObject = appendDataObject;
    this.logger = logger;
  }

  public final CachedStatement<T> appendDispatchableStatement(Connection connection) {
    return createStatement(appendDispatchableExpression(), appendDataObject, connection, false, logger);
  }

  public final CachedStatement<T> appendEntryStatement(Connection connection) {
    return createStatement(appendEntryExpression(), appendDataObject, connection, false, logger);
  }

  public final CachedStatement<T> appendBatchEntriesStatement(Connection connection) {
    return createStatement(appendEntryExpression(), appendDataObject, connection, true, logger);
  }

  public final CachedStatement<T> appendEntryIdentityStatement(Connection connection) {
    return createStatement(appendEntryIdentityExpression(), null, connection, false, logger);
  }

  public final CachedStatement<T> deleteStatement(Connection connection) {
    return createStatement(deleteDispatchableExpression(), null, connection, false, logger);
  }

  public final CachedStatement<T> queryAllStatement(Connection connection) {
    return prepareQuery(createStatement(selectDispatchableExpression(), null, connection, false, logger), originatorId, logger);
  }

  public CachedStatement<T> getQueryEntry(Connection connection) {
    return createStatement(queryEntryExpression(), appendDataObject, connection, false, logger);
  }

  protected abstract String appendEntryExpression();
  protected abstract String queryEntryExpression();

  protected abstract String appendDispatchableExpression();
  protected abstract String appendEntryIdentityExpression();
  protected abstract String deleteDispatchableExpression();
  protected abstract String selectDispatchableExpression();

  private CachedStatement<T> createStatement(
          final String sql,
          final T data,
          final Connection connection,
          boolean batchInsert,
          final Logger logger) {

    try {
      final PreparedStatement preparedStatement = batchInsert
              ? connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS) // if batch insert return generated key
              : connection.prepareStatement(sql);
      return new CachedStatement<T>(preparedStatement, data);
    } catch (Exception e) {
      final String message =
              getClass().getSimpleName() + ": Failed to create dispatchable statement: \n" +
              sql +
              "\nbecause: " + e.getMessage();
      logger.error(message, e);
      throw new IllegalStateException(message);
    }
  }

  private CachedStatement<T> prepareQuery(final CachedStatement<T> cached, String originatorId, final Logger logger) {
    try {
      cached.preparedStatement.setString(1, originatorId);
      return cached;
    } catch (Exception e) {
      final String message =
              getClass().getSimpleName() + ": Failed to prepare query=all because: " + e.getMessage();
      logger.error(message, e);
      throw new IllegalStateException(message);
    }
  }
}
