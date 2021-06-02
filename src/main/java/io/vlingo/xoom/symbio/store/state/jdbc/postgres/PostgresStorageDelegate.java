// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.jdbc.postgres;

import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.actors.ActorInstantiator;
import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.EntryReader;
import io.vlingo.xoom.symbio.store.EntryReader.Advice;
import io.vlingo.xoom.symbio.store.common.jdbc.CachedStatement;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.state.StateStore.StorageDelegate;
import io.vlingo.xoom.symbio.store.state.jdbc.DbStateStoreEntryReaderActor;
import io.vlingo.xoom.symbio.store.state.jdbc.DbStateStoreEntryReaderActor.DbStateStoreEntryReaderInstantiator;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCDispatchableCachedStatements;
import io.vlingo.xoom.symbio.store.state.jdbc.JDBCStorageDelegate;
import org.postgresql.util.PGobject;

import java.sql.ResultSet;
import java.text.MessageFormat;

public class PostgresStorageDelegate extends JDBCStorageDelegate<Object> implements StorageDelegate, PostgresQueries {
  private final Configuration configuration;

  public PostgresStorageDelegate(final Configuration configuration, final Logger logger) {
    super(configuration.connectionProvider,
        configuration.format,
        configuration.originatorId,
        configuration.createTables,
        logger);

    this.configuration = configuration;
  }

  @Override
  public StorageDelegate copy() {
    try {
      return new PostgresStorageDelegate(Configuration.cloneOf(configuration), logger);
    } catch (Exception e) {
      final String message = "Copy of StorageDelegate failed because: " + e.getMessage();
      logger.error(message, e);
      throw new IllegalStateException(message, e);
    }
  }

  @Override
  public Advice entryReaderAdvice() {
    try {
      return new EntryReader.Advice(
          Configuration.cloneOf(configuration),
          DbStateStoreEntryReaderActor.class,
          namedEntry(SQL_QUERY_ENTRY_BATCH),
          namedEntry(SQL_QUERY_ENTRY_IDS),
          namedEntry(SQL_QUERY_ENTRY),
          namedEntry(QUERY_COUNT),
          namedEntryOffsets(QUERY_LATEST_OFFSET),
          namedEntryOffsets(UPDATE_CURRENT_OFFSET));
    } catch (Exception e) {
      throw new IllegalStateException("Cannot create EntryReader.Advice because: " + e.getMessage(), e);
    }
  }

  @Override
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public <A extends Actor> ActorInstantiator<A> instantiator() {
    return new DbStateStoreEntryReaderInstantiator();
  }

  @Override
  protected byte[] binaryDataFrom(final ResultSet resultSet, final int columnIndex) throws Exception {
    final byte[] data = resultSet.getBytes(columnIndex);
    return data;
  }

  @Override
  protected <D> D binaryDataTypeObject() throws Exception {
    return null;
  }

  @Override
  protected JDBCDispatchableCachedStatements<Object> dispatchableCachedStatements() {
    return new PostgresDispatchableCachedStatements<Object>(originatorId, format, logger);
  }

  @Override
  protected String dispatchableIdIndexCreateExpression() {
    return namedDispatchable(SQL_DISPATCH_ID_INDEX);
  }

  @Override
  protected String dispatchableOriginatorIdIndexCreateExpression() {
    return namedDispatchable(SQL_ORIGINATOR_ID_INDEX);
  }

  @Override
  protected String dispatchableTableCreateExpression() {
    return MessageFormat.format(SQL_CREATE_DISPATCHABLES_STORE, dispatchableTableName(),
        format.isBinary() ? SQL_FORMAT_BINARY : SQL_FORMAT_TEXT1); // TODO: SQL_FORMAT_TEXT2
  }

  @Override
  protected String dispatchableTableName() {
    return TBL_VLINGO_SYMBIO_DISPATCHABLES;
  }

  @Override
  protected String entryTableCreateExpression() {
    return MessageFormat.format(SQL_CREATE_ENTRY_STORE, entryTableName(),
        format.isBinary() ? SQL_FORMAT_BINARY : SQL_FORMAT_TEXT1);
  }

  @Override
  protected String entryOffsetsTableCreateExpression() {
    return MessageFormat.format(SQL_CREATE_ENTRY_STORE_OFFSETS, entryOffsetsTableName());
  }

  @Override
  protected String entryTableName() {
    return TBL_VLINGO_SYMBIO_STATE_ENTRY;
  }

  @Override
  protected String entryOffsetsTableName() {
    return TBL_VLINGO_SYMBIO_STATE_ENTRY_OFFSETS;
  }

  @Override
  protected String readAllExpression(final String storeName) {
    return MessageFormat.format(SQL_ALL_STATE_READ, storeName.toLowerCase());
  }

  @Override
  protected String readExpression(final String storeName, final String id) {
    return MessageFormat.format(SQL_STATE_READ, storeName.toLowerCase());
  }

  @Override
  protected <E> void setBinaryObject(final CachedStatement<Object> cached, final int columnIndex, final Entry<E> entry) throws Exception {
    cached.preparedStatement.setBytes(columnIndex, (byte[]) entry.entryData());
  }

  @Override
  protected <S> void setBinaryObject(final CachedStatement<Object> cached, int columnIndex, final State<S> state) throws Exception {
    cached.preparedStatement.setBytes(columnIndex, (byte[]) state.data);
  }

  @Override
  protected <E> void setTextObject(final CachedStatement<Object> cached, final int columnIndex, final Entry<E> entry) throws Exception {
    final PGobject jsonObject = new PGobject();
    jsonObject.setType("json");
    jsonObject.setValue((String) entry.entryData());
    cached.preparedStatement.setObject(columnIndex, jsonObject);
  }

  @Override
  protected <S> void setTextObject(final CachedStatement<Object> cached, int columnIndex, State<S> state) throws Exception {
    final PGobject jsonObject = new PGobject();
    jsonObject.setType("json");
    jsonObject.setValue((String) state.data);
    cached.preparedStatement.setObject(columnIndex, jsonObject);
  }

  @Override
  protected String stateStoreTableCreateExpression(final String stateName) {
    return MessageFormat.format(SQL_CREATE_STATE_STORE, stateName,
        format.isBinary() ? SQL_FORMAT_BINARY : SQL_FORMAT_TEXT1); // TODO: SQL_FORMAT_TEXT2
  }

  @Override
  protected String tableNameFor(final String storeName) {
    return "tbl_" + storeName.toLowerCase();
  }

  @Override
  protected String textDataFrom(final ResultSet resultSet, final int columnIndex) throws Exception {
    final String text = resultSet.getObject(columnIndex).toString();
    return text;
  }

  @Override
  protected String writeExpression(final String storeName) {
    return MessageFormat.format(SQL_STATE_WRITE, storeName.toLowerCase(),
        format.isBinary() ? SQL_FORMAT_BINARY_CAST : SQL_FORMAT_TEXT_CAST);
  }

  private String namedDispatchable(final String sql) {
    return MessageFormat.format(sql, dispatchableTableName());
  }

  private String namedEntry(final String sql) {
    return MessageFormat.format(sql, entryTableName());
  }

  private String namedEntryOffsets(final String sql) {
    return MessageFormat.format(sql, entryOffsetsTableName());
  }

  class PostgresDispatchableCachedStatements<T> extends JDBCDispatchableCachedStatements<T> {
    PostgresDispatchableCachedStatements(
        final String originatorId,
        final DataFormat format,
        final Logger logger) {

      super(originatorId, format, null, logger);
    }

    @Override
    protected String appendDispatchableExpression() {
      return namedDispatchable(SQL_DISPATCHABLE_APPEND);
    }

    @Override
    protected String deleteDispatchableExpression() {
      return namedDispatchable(SQL_DISPATCHABLE_DELETE);
    }

    @Override
    protected String selectDispatchableExpression() {
      return namedDispatchable(SQL_DISPATCHABLE_SELECT);
    }

    @Override
    protected String appendEntryExpression() {
      return namedEntry(SQL_APPEND_ENTRY);
    }

    @Override
    protected String queryEntryExpression() {
      return namedEntry(SQL_QUERY_ENTRY);
    }

    @Override
    protected String appendEntryIdentityExpression() {
      return SQL_APPEND_ENTRY_IDENTITY;
    }
  }
}
