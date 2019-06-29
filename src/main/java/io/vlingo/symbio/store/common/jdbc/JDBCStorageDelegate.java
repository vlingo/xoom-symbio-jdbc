// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.common.jdbc;

import io.vlingo.actors.Logger;
import io.vlingo.common.Tuple2;
import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.BinaryState;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.state.StateStore.StorageDelegate;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
import io.vlingo.symbio.store.state.jdbc.CachedStatement;
import io.vlingo.symbio.store.state.jdbc.JDBCDispatchableCachedStatements;
import io.vlingo.symbio.store.state.jdbc.Mode;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class JDBCStorageDelegate<T> implements StorageDelegate,
        DispatcherControl.DispatcherControlDelegate<Entry<?>, State<?>> {
  protected final Connection connection;
  protected final JDBCDispatchableCachedStatements<T> dispatchableCachedStatements;
  protected final DataFormat format;
  protected final Logger logger;
  protected Mode mode;
  protected final String originatorId;
  protected final Map<String, CachedStatement<T>> readStatements;
  protected final Map<String, CachedStatement<T>> writeStatements;

  protected JDBCStorageDelegate(
          final Connection connection,
          final DataFormat format,
          final String originatorId,
          final boolean createTables,
          final Logger logger) {

    this.connection = connection;
    this.format = format;
    this.originatorId = originatorId;
    this.logger = logger;
    this.mode = Mode.None;
    if (createTables) createTables();
    this.dispatchableCachedStatements = dispatchableCachedStatements();
    this.readStatements = new HashMap<>();
    this.writeStatements = new HashMap<>();
  }

  @SuppressWarnings("unchecked")
  public <A, E> A appendExpressionFor(final Entry<E> entry) throws Exception {
    final CachedStatement<T> cachedStatement = dispatchableCachedStatements.appendEntryStatement();
    prepareForAppend(cachedStatement, entry);
    return (A) cachedStatement.preparedStatement;
  }

  @SuppressWarnings("unchecked")
  public <A> A appendIdentityExpression() {
    final CachedStatement<T> cachedStatement = dispatchableCachedStatements.appendEntryIdentityStatement();
    return (A) cachedStatement.preparedStatement;
  }

  @Override
  public Collection<Dispatchable<Entry<?>, State<?>>> allUnconfirmedDispatchableStates() throws Exception {
    final List<Dispatchable<Entry<?>, State<?>>> dispatchables = new ArrayList<>();

    try (final ResultSet result = dispatchableCachedStatements.queryAllStatement().preparedStatement.executeQuery()) {
      while (result.next()) {
        final Dispatchable<Entry<?>, State<?>> dispatchable = dispatchableFrom(result);
        dispatchables.add(dispatchable);
      }
    }

    return dispatchables;
  }

  public void beginRead() {
    if (mode != Mode.None) {
      logger.warn(getClass().getSimpleName() + ": Cannot begin read because currently: " + mode.name());
    } else {
      mode = Mode.Reading;
    }
  }

  public void beginWrite() throws Exception {
    if (mode != Mode.None) {
//      System.out.println("ALREADY IN WRITING MODE");
//      (new IllegalStateException()).printStackTrace();
      logger.warn(getClass().getSimpleName() + ": Cannot begin write because currently: " + mode.name());
    } else {
//      System.out.println("SET WRITING MODE");
//      (new IllegalStateException()).printStackTrace();
      mode = Mode.Writing;
    }
  }

  @Override
  public void stop() {
    close();
  }

  @Override
  public void close() {
    try {
      mode = Mode.None;
      final Connection connection = connection();
      if (connection != null) {
        connection.close();
      }
    } catch (final Exception e) {
      logger.error(getClass().getSimpleName() + ": Could not close because: " + e.getMessage(), e);
    }
  }

  @Override
  public boolean isClosed() {
    try {
      return connection == null || connection.isClosed();
    }
    catch (final SQLException ex) {
      return true;
    }
  }

  public void complete() throws Exception {
    mode = Mode.None;
    connection.commit();
  }

  @SuppressWarnings("unchecked")
  public <C> C connection() {
    return (C) connection;
  }

  @Override
  public void confirmDispatched(final String dispatchId) {
    try {
      beginWrite();
      dispatchableCachedStatements.deleteStatement().preparedStatement.clearParameters();
      dispatchableCachedStatements.deleteStatement().preparedStatement.setString(1, dispatchId);
      dispatchableCachedStatements.deleteStatement().preparedStatement.executeUpdate();
      complete();
    } catch (final Exception e) {
      fail();
      logger.error(getClass().getSimpleName() +
              ": Confirm dispatched for: " + dispatchId +
              " failed because: " + e.getMessage(), e);
    }
  }

  @SuppressWarnings("unchecked")
  public <W, S> W dispatchableWriteExpressionFor(final String dispatchId, final State<S> state) throws Exception {
    final PreparedStatement preparedStatement = dispatchableCachedStatements.appendDispatchableStatement().preparedStatement;

    preparedStatement.clearParameters();
    preparedStatement.setObject(1, Timestamp.valueOf(LocalDateTime.now()));
    preparedStatement.setString(2, originatorId);
    preparedStatement.setString(3, dispatchId);
    preparedStatement.setString(4, state.id);
    preparedStatement.setString(5, state.type);
    preparedStatement.setInt(6, state.typeVersion);
    if (format.isBinary()) {
      setBinaryObject(dispatchableCachedStatements.appendDispatchableStatement(), 7, state);
    } else if (state.isText()) {
      setTextObject(dispatchableCachedStatements.appendDispatchableStatement(), 7, state);
    }
    preparedStatement.setInt(8, state.dataVersion);
    preparedStatement.setString(9, state.metadata.value);
    preparedStatement.setString(10, state.metadata.operation);
    final Tuple2<String, String> metadataObject = serialized(state.metadata.object);
    preparedStatement.setString(11, metadataObject._1);
    preparedStatement.setString(12, metadataObject._2);
    return (W) preparedStatement;
  }

  public void fail() {
    try {
      mode = Mode.None;
      connection.rollback();
    } catch (final Exception e) {
      logger.error(getClass().getSimpleName() + ": Rollback failed because: " + e.getMessage(), e);
    }
  }

  @Override
  public String originatorId() {
    return originatorId;
  }

  @SuppressWarnings("unchecked")
  public <R> R readExpressionFor(final String storeName, final String id) throws Exception {
    final CachedStatement<T> maybeCached = readStatements.get(storeName);

    if (maybeCached == null) {
      final String select = readExpression(storeName, id);
      final PreparedStatement preparedStatement = connection.prepareStatement(select);
      final CachedStatement<T> cached = new CachedStatement<>(preparedStatement, null);
      readStatements.put(storeName, cached);
      prepareForRead(cached, id);
      return (R) preparedStatement;
    }

    prepareForRead(maybeCached, id);

    return (R) maybeCached.preparedStatement;
  }

  public <S> S session() throws Exception {
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <S, R> S stateFrom(final R result, final String id) throws Exception {
    final ResultSet resultSet = ((ResultSet) result);
    if (!resultSet.next()) {
      return (S) (format.isBinary() ? new BinaryState() : new TextState());
    }
    final Class<?> type = Class.forName(resultSet.getString(1));
    final int typeVersion = resultSet.getInt(2);
    // 3 below
    final int dataVersion = resultSet.getInt(4);
    final String metadataValue = resultSet.getString(5);
    final String metadataOperation = resultSet.getString(6);

    final Metadata metadata = Metadata.with(metadataValue, metadataOperation);

    // note possible truncation with long cast to in, but
    // hopefully no objects are larger than int max value

    if (format.isBinary()) {
      final byte[] data = binaryDataFrom(resultSet, 3);
      return (S) new BinaryState(id, type, typeVersion, data, dataVersion, metadata);
    } else {
      final String data = textDataFrom(resultSet, 3);
      return (S) new TextState(id, type, typeVersion, data, dataVersion, metadata);
    }
  }

  @SuppressWarnings("unchecked")
  public <W, S> W writeExpressionFor(final String storeName, final State<S> state) throws Exception {
    final CachedStatement<T> maybeCached = writeStatements.get(storeName);

    if (maybeCached == null) {
      final String upsert = writeExpression(storeName);
      final PreparedStatement preparedStatement = connection.prepareStatement(upsert);
      final CachedStatement<T> cached = new CachedStatement<>(preparedStatement, binaryDataTypeObject());
      writeStatements.put(storeName, cached);
      prepareForWrite(cached, state);
      return (W) cached.preparedStatement;
    }

    prepareForWrite(maybeCached, state);

    return (W) maybeCached.preparedStatement;
  }

  protected abstract byte[] binaryDataFrom(final ResultSet resultSet, final int columnIndex) throws Exception;
  protected abstract <D> D binaryDataTypeObject() throws Exception;
  protected abstract JDBCDispatchableCachedStatements<T> dispatchableCachedStatements();
  protected abstract String dispatchableIdIndexCreateExpression();
  protected abstract String dispatchableOriginatorIdIndexCreateExpression();
  protected abstract String dispatchableTableCreateExpression();
  protected abstract String dispatchableTableName();
  protected abstract String entryTableCreateExpression();
  protected abstract String entryTableName();
  protected abstract String readExpression(final String storeName, final String id);
  protected abstract <S> void setBinaryObject(final CachedStatement<T> cached, int columnIndex, final State<S> state) throws Exception;
  protected abstract <E> void setBinaryObject(final CachedStatement<T> cached, int columnIndex, final Entry<E> entry) throws Exception;
  protected abstract <S> void setTextObject(final CachedStatement<T> cached, int columnIndex, final State<S> state) throws Exception;
  protected abstract <E> void setTextObject(final CachedStatement<T> cached, int columnIndex, final Entry<E> entry) throws Exception;
  protected abstract String stateStoreTableCreateExpression(final String storeName);
  protected abstract String tableNameFor(final String storeName);
  protected abstract String textDataFrom(final ResultSet resultSet, final int columnIndex) throws Exception;
  protected abstract String writeExpression(final String storeName);

  private void createDispatchablesTable() throws Exception {
    final String tableName = dispatchableTableName();
    if (!tableExists(tableName)) {
      try (final Statement statement = connection.createStatement()) {
        statement.executeUpdate(dispatchableTableCreateExpression());
        statement.executeUpdate(dispatchableIdIndexCreateExpression());
        statement.executeUpdate(dispatchableOriginatorIdIndexCreateExpression());
        connection.commit();
      } catch (final Exception e) {
        throw new IllegalStateException("Cannot create table " + tableName + " because: " + e, e);
      }
    }
  }

  private void createEntryTable() throws Exception {
    final String tableName = entryTableName();
    if (!tableExists(tableName)) {
      try (final Statement statement = connection.createStatement()) {
        statement.executeUpdate(entryTableCreateExpression());
        connection.commit();
      } catch (final Exception e) {
        throw new IllegalStateException("Cannot create table " + tableName + " because: " + e, e);
      }
    }
  }

  private void createStateStoreTable(final String storeName) throws Exception {
    final String sql = stateStoreTableCreateExpression(storeName);
    try (final Statement statement = connection.createStatement()) {
      statement.executeUpdate(sql);
      connection.commit();
    }
  }

  private void createTables() {
    try {
      createDispatchablesTable();
    } catch (final Exception e) {
      // assume table exists; could look at metadata
      logger.error("Could not create dispatchables table because: " + e.getMessage(), e);
    }

    try {
      createEntryTable();
    } catch (final Exception e) {
      // assume table exists; could look at metadata
      logger.error("Could not create entry table because: " + e.getMessage(), e);
    }

    for (final String storeName : StateTypeStateStoreMap.allStoreNames()) {
      final String tableName = tableNameFor(storeName);
      try {
        if (!tableExists(tableName)) {
          createStateStoreTable(storeName);
        }
      } catch (final Exception e) {
        // assume table exists; could look at metadata
        logger.error("Could not create " + tableName + " table because: " + e.getMessage(), e);
      }
    }
  }

  private void prepareForRead(final CachedStatement<T> cached, final String id) throws Exception {
    cached.preparedStatement.clearParameters();
    cached.preparedStatement.setString(1, id);
  }

  private <E> void prepareForAppend(final CachedStatement<T> cached, final Entry<E> entry) throws Exception {
    cached.preparedStatement.clearParameters();
    cached.preparedStatement.setString(1, entry.type());
    cached.preparedStatement.setInt(2, entry.typeVersion());
    if (format.isBinary()) {
      this.setBinaryObject(cached, 3, entry);
    } else if (format.isText()) {
      this.setTextObject(cached, 3, entry);
    }
    cached.preparedStatement.setString(4, entry.metadata().value);
    cached.preparedStatement.setString(5, entry.metadata().operation);
  }

  private <S> void prepareForWrite(final CachedStatement<T> cached, final State<S> state) throws Exception {
    cached.preparedStatement.clearParameters();

    cached.preparedStatement.setString(1, state.id);
    cached.preparedStatement.setString(2, state.type);
    cached.preparedStatement.setInt(3, state.typeVersion);
    if (format.isBinary()) {
      this.setBinaryObject(cached, 4, state);
    } else if (state.isText()) {
      this.setTextObject(cached, 4, state);
    }
    cached.preparedStatement.setInt(5, state.dataVersion);
    cached.preparedStatement.setString(6, state.metadata.value);
    cached.preparedStatement.setString(7, state.metadata.operation);
  }

  private Tuple2<String, String> serialized(final Object object) {
    if (object != null) {
      return Tuple2.from(JsonSerialization.serialized(object), object.getClass().getName());
    }
    return Tuple2.from(null, null);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private <S extends State<?>> Dispatchable<Entry<?>, S> dispatchableFrom(final ResultSet resultSet) throws Exception {
    final LocalDateTime createdAt = resultSet.getTimestamp(1).toLocalDateTime();
    final String dispatchId = resultSet.getString(2);
    final String id = resultSet.getString(3);
    final Class<?> type = Class.forName(resultSet.getString(4));
    final int typeVersion = resultSet.getInt(5);
    // 6 below
    final int dataVersion = resultSet.getInt(7);
    final String metadataValue = resultSet.getString(8);
    final String metadataOperation = resultSet.getString(9);
    final String metadataObject = resultSet.getString(10);
    final String metadataObjectType = resultSet.getString(11);

    final Object object = metadataObject != null ?
            JsonSerialization.deserialized(metadataObject, Class.forName(metadataObjectType)) : null;

    final Metadata metadata = Metadata.with(object, metadataValue, metadataOperation);

    final S state;
    if (format.isBinary()) {
      final byte[] data = binaryDataFrom(resultSet, 6);
      state = ((S) new BinaryState(id, type, typeVersion, data, dataVersion, metadata));
    } else {
      final String data = textDataFrom(resultSet, 6);
      state = ((S) new TextState(id, type, typeVersion, data, dataVersion, metadata));
    }
    return new Dispatchable<>(dispatchId, createdAt, state, Collections.emptyList());
  }

  private boolean tableExists(final String tableName) throws Exception {
    final DatabaseMetaData metadata = connection.getMetaData();
    try (final ResultSet resultSet = metadata.getTables(null, null, tableName, null)) {
      return resultSet.next();
    }
  }
}
