// Copyright © 2012-2022 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.jdbc;

import static io.vlingo.xoom.symbio.Entry.typed;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.common.Tuple2;
import io.vlingo.xoom.common.serialization.JsonSerialization;
import io.vlingo.xoom.symbio.BaseEntry;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.State.BinaryState;
import io.vlingo.xoom.symbio.State.TextState;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.QueryExpression;
import io.vlingo.xoom.symbio.store.StoredTypes;
import io.vlingo.xoom.symbio.store.common.jdbc.CachedStatement;
import io.vlingo.xoom.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.state.StateStore.StorageDelegate;
import io.vlingo.xoom.symbio.store.state.StateTypeStateStoreMap;

public abstract class JDBCStorageDelegate<T> implements StorageDelegate,
    DispatcherControl.DispatcherControlDelegate<Entry<?>, State<?>> {
  private static final String DISPATCHEABLE_ENTRIES_DELIMITER = "|";

  protected final Connection connection;
  protected final ConnectionProvider connectionProvider;
  protected final boolean createTables;
  private final JDBCDispatchableCachedStatements<T> dispatchableCachedStatements;
  protected final DataFormat format;
  protected final Logger logger;
  protected Mode mode;
  protected final String originatorId;
  protected final Map<String, CachedStatement<T>> readStatements;
  protected final Map<String, CachedStatement<T>> writeStatements;

  protected JDBCStorageDelegate(
      final ConnectionProvider connectionProvider,
      final DataFormat format,
      final String originatorId,
      final boolean createTables,
      final Logger logger) {

    this.connectionProvider = connectionProvider;
    this.connection = connectionProvider.newConnection();
    this.format = format;
    this.originatorId = originatorId;
    this.logger = logger;
    this.mode = Mode.None;
    this.createTables = createTables;
    if (createTables) createTables();
    this.dispatchableCachedStatements = dispatchableCachedStatements();
    this.readStatements = new HashMap<>();
    this.writeStatements = new HashMap<>();
  }

  @SuppressWarnings("unchecked")
  public <A, E> A appendExpressionFor(final Entry<E> entry) throws Exception {
    final CachedStatement<T> cachedStatement = dispatchableCachedStatements.appendEntryStatement(connection);
    prepareForAppend(cachedStatement, entry);
    return (A) cachedStatement.preparedStatement;
  }

  @SuppressWarnings("unchecked")
  public <A> A appendExpressionFor(final List<Entry<?>> entries) throws Exception {
    final CachedStatement<T> cachedStatement = dispatchableCachedStatements.appendBatchEntriesStatement(connection);
    prepareForBatchAppend(cachedStatement, entries);
    return (A) cachedStatement.preparedStatement;
  }

  @SuppressWarnings("unchecked")
  public <A> A appendIdentityExpression() {
    final CachedStatement<T> cachedStatement = dispatchableCachedStatements.appendEntryIdentityStatement(connection);
    return (A) cachedStatement.preparedStatement;
  }

  @Override
  public Collection<Dispatchable<Entry<?>, State<?>>> allUnconfirmedDispatchableStates() throws Exception {
    final List<Dispatchable<Entry<?>, State<?>>> dispatchables = new ArrayList<>();

    try (final ResultSet resultSet = dispatchableCachedStatements.queryAllStatement(connection).preparedStatement.executeQuery()) {
      while (resultSet.next()) {
        final Dispatchable<Entry<?>, State<?>> dispatchable = dispatchableFrom(resultSet);
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
    } catch (final SQLException ex) {
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
      PreparedStatement deleteStatement = dispatchableCachedStatements.deleteStatement(connection).preparedStatement;
      deleteStatement.clearParameters();
      deleteStatement.setString(1, dispatchId);
      deleteStatement.executeUpdate();
      complete();
    } catch (final Exception e) {
      fail();
      logger.error(getClass().getSimpleName() +
          ": Confirm dispatched for: " + dispatchId +
          " failed because: " + e.getMessage(), e);
    }
  }

  @SuppressWarnings("unchecked")
  public <W, S> W dispatchableWriteExpressionFor(final Dispatchable<Entry<?>, State<S>> dispatchable) throws Exception {
    CachedStatement<T> cachedStatement = dispatchableCachedStatements.appendDispatchableStatement(connection);
    dispatchableWriteExpressionFor(cachedStatement, dispatchable);

    return (W) cachedStatement.preparedStatement;
  }

  public PreparedStatement dispatchableWriteExpressionFor(final List<Dispatchable<Entry<?>, State<String>>> dispatchables) throws Exception {
    CachedStatement<T> cachedStatement = dispatchableCachedStatements.appendDispatchableStatement(connection);
    final PreparedStatement preparedStatement = cachedStatement.preparedStatement;
    for (Dispatchable<Entry<?>, State<String>> dispatchable : dispatchables) {
      dispatchableWriteExpressionFor(cachedStatement, dispatchable);
      preparedStatement.addBatch();
    }

    return preparedStatement;
  }

  @SuppressWarnings({"unchecked"})
  private <S extends State<?>> Dispatchable<Entry<?>, S> dispatchableFrom(final ResultSet resultSet) throws Exception {
    final LocalDateTime createdAt = resultSet.getTimestamp(1).toLocalDateTime();
    final String dispatchId = resultSet.getString(2);
    final String id = resultSet.getString(3);
    final Class<?> type = StoredTypes.forName(resultSet.getString(4));
    final int typeVersion = resultSet.getInt(5);
    // 6 below
    final int dataVersion = resultSet.getInt(7);
    final String metadataValue = resultSet.getString(8);
    final String metadataOperation = resultSet.getString(9);
    final String metadataObject = resultSet.getString(10);
    final String metadataObjectType = resultSet.getString(11);

    final Object object = metadataObject != null ?
        JsonSerialization.deserialized(metadataObject, StoredTypes.forName(metadataObjectType)) : null;

    final Metadata metadata = Metadata.with(object, metadataValue, metadataOperation);

    final S state;
    if (format.isBinary()) {
      final byte[] data = binaryDataFrom(resultSet, 6);
      state = ((S) new BinaryState(id, type, typeVersion, data, dataVersion, metadata));
    } else {
      final String data = textDataFrom(resultSet, 6);
      state = ((S) new TextState(id, type, typeVersion, data, dataVersion, metadata));
    }

    final String entriesIds = resultSet.getString(12);
    final List<Entry<?>> entries = new ArrayList<>();
    if (entriesIds != null && !entriesIds.isEmpty()) {
      final String[] ids = entriesIds.split("\\" + DISPATCHEABLE_ENTRIES_DELIMITER);
      for (final String entryId : ids) {
        final PreparedStatement queryEntryStatement = dispatchableCachedStatements.getQueryEntry(connection).preparedStatement;
        queryEntryStatement.clearParameters();
        queryEntryStatement.setObject(1, Long.valueOf(entryId));
        queryEntryStatement.executeQuery();
        try (final ResultSet result = queryEntryStatement.executeQuery()) {
          if (result.next()) {
            entries.add(entryFrom(result, entryId));
          }
        }
        queryEntryStatement.clearParameters();
      }
    }
    return new Dispatchable<>(dispatchId, createdAt, state, entries);
  }

  private <S> void dispatchableWriteExpressionFor(final CachedStatement<T> cachedStatement, final Dispatchable<Entry<?>, State<S>> dispatchable) throws Exception {
    final PreparedStatement preparedStatement = cachedStatement.preparedStatement;
    final State<S> state = dispatchable.typedState();

    preparedStatement.clearParameters();
    preparedStatement.setObject(1, Timestamp.valueOf(dispatchable.createdOn()));
    preparedStatement.setString(2, originatorId);
    preparedStatement.setString(3, dispatchable.id());
    preparedStatement.setString(4, state.id);
    preparedStatement.setString(5, state.type);
    preparedStatement.setInt(6, state.typeVersion);
    if (format.isBinary()) {
      setBinaryObject(cachedStatement, 7, state);
    } else if (state.isText()) {
      setTextObject(cachedStatement, 7, state);
    }
    preparedStatement.setInt(8, state.dataVersion);
    preparedStatement.setString(9, state.metadata.value);
    preparedStatement.setString(10, state.metadata.operation);
    final Tuple2<String, String> metadataObject = serialized(state.metadata.object);
    preparedStatement.setString(11, metadataObject._1);
    preparedStatement.setString(12, metadataObject._2);
    if (dispatchable.entries() != null && !dispatchable.entries().isEmpty()) {
      preparedStatement.setString(13,
          dispatchable.entries()
              .stream()
              .map(Entry::id)
              .collect(Collectors.joining(DISPATCHEABLE_ENTRIES_DELIMITER))
      );
    } else {
      preparedStatement.setString(13, "");
    }
  }

  private Entry<?> entryFrom(final ResultSet result, final String id) throws Exception {
    final String type = result.getString(2);
    final int typeVersion = result.getInt(3);
    final String metadataValue = result.getString(5);
    final String metadataOperation = result.getString(6);

    final Metadata metadata = Metadata.with(metadataValue, metadataOperation);

    if (format.isBinary()) {
      return new BaseEntry.BinaryEntry(id, typed(type), typeVersion, binaryDataFrom(result, 4), metadata);
    } else {
      return new BaseEntry.TextEntry(id, typed(type), typeVersion, textDataFrom(result, 4), metadata);
    }
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
  public void initialize() {
    logger.info("============================================================");
    logger.info("=============== CREATE TABLES: " + createTables);
    logger.info("============================================================");
    if (createTables) createTables();
    logger.info("============================================================");
  }

  @Override
  public String originatorId() {
    return originatorId;
  }

  @SuppressWarnings("unchecked")
  public <R> R createReadAllExpressionFor(final String storeName) {
    final String select = readAllExpression(storeName);
    final BiFunction<Connection, String, PreparedStatement> statementFactory = (connection, query) -> {
      try {
        return connection.prepareStatement(
            select,
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
      } catch (Exception e) {
        String message = "Failed to create readAllExpression because: " + e.getMessage();
        logger.error(message, e);
        throw new RuntimeException(message, e);
      }
    };

    return (R) new QueryResource(select, connectionProvider, statementFactory);
  }

  @SuppressWarnings("unchecked")
  public <R> R createReadExpressionFor(final String storeName, final String id) {
    final String select = readExpression(storeName, id);
    final BiFunction<Connection, String, PreparedStatement> statementFactory = (connection, query) -> {
      try {
        PreparedStatement preparedStatement = connection.prepareStatement(
            query,
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);
        preparedStatement.setString(1, id);

        return preparedStatement;
      } catch (Exception e) {
        String message = "Failed to create readExpression because: " + e.getMessage();
        logger.error(message, e);
        throw new RuntimeException(message, e);
      }
    };

    return (R) new QueryResource(select, connectionProvider, statementFactory);
  }

  @SuppressWarnings("unchecked")
  public <R> R readSomeExpressionFor(final String storeName, final QueryExpression queryExpression) throws Exception {
    final String select = readSomeExpression(storeName, queryExpression.query);
    final BiFunction<Connection, String, PreparedStatement> statementFactory = (connection, query) -> {
      try {
        PreparedStatement preparedStatement = connection.prepareStatement(
            select,
            ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);

        if (queryExpression.isListQueryExpression()) {
          setStatementArguments(preparedStatement, queryExpression.asListQueryExpression().parameters);
        }

        return preparedStatement;
      } catch (Exception e) {
        String message = "Failed to create readSomeExpression because: " + e.getMessage();
        logger.error(message, e);
        throw new RuntimeException(message, e);
      }
    };

    return (R) new QueryResource(select, connectionProvider, statementFactory);
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
    return stateFrom(resultSet, id, 0);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <S, R> S stateFrom(final R result, final String id, final int columnOffset) throws Exception {
    final ResultSet resultSet = ((ResultSet) result);
    final Class<?> type = StoredTypes.forName(resultSet.getString(1 + columnOffset));
    final int typeVersion = resultSet.getInt(2 + columnOffset);
    // 3 below
    final int dataVersion = resultSet.getInt(4 + columnOffset);
    final String metadataValue = resultSet.getString(5 + columnOffset);
    final String metadataOperation = resultSet.getString(6 + columnOffset);

    final Metadata metadata = Metadata.with(metadataValue, metadataOperation);

    // note possible truncation with long cast to in, but
    // hopefully no objects are larger than int max value

    if (format.isBinary()) {
      final byte[] data = binaryDataFrom(resultSet, 3 + columnOffset);
      return (S) new BinaryState(id, type, typeVersion, data, dataVersion, metadata);
    } else {
      final String data = textDataFrom(resultSet, 3 + columnOffset);
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

  @SuppressWarnings("unchecked")
  public <W> W writeExpressionFor(final String storeName, final List<? extends State<?>> states) throws Exception {
    CachedStatement<T> maybeCached = writeStatements.get(storeName);

    if (maybeCached == null) {
      final String upsert = writeExpression(storeName);
      final PreparedStatement preparedStatement = connection.prepareStatement(upsert);
      maybeCached = new CachedStatement<>(preparedStatement, binaryDataTypeObject());
      writeStatements.put(storeName, maybeCached);
    }

    prepareForBatchWrite(maybeCached, states);

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

  protected abstract String entryOffsetsTableName();

  protected abstract String entryOffsetsTableCreateExpression();

  protected abstract String readAllExpression(final String storeName);

  protected abstract String readExpression(final String storeName, final String id);

  protected abstract <S> void setBinaryObject(final CachedStatement<T> cached, int columnIndex, final State<S> state) throws Exception;

  protected abstract <E> void setBinaryObject(final CachedStatement<T> cached, int columnIndex, final Entry<E> entry) throws Exception;

  protected abstract <S> void setTextObject(final CachedStatement<T> cached, int columnIndex, final State<S> state) throws Exception;

  protected abstract <E> void setTextObject(final CachedStatement<T> cached, int columnIndex, final Entry<E> entry) throws Exception;

  protected abstract String stateStoreTableCreateExpression(final String tableName);

  protected abstract String tableNameFor(final String storeName);

  protected abstract String textDataFrom(final ResultSet resultSet, final int columnIndex) throws Exception;

  protected abstract String writeExpression(final String storeName);

  protected String readSomeExpression(final String storeName, final String expression) {
    return MessageFormat.format(expression, this.tableNameFor(storeName));
  }

  /**
   * Answer the {@code catalogName} of the database, which by default is the {@code databaseName}.
   * 
   * <p>NOTE: Override for subclass specialized implementation
   *
   * @return String
   */
  protected String catalogName() {
    return connectionProvider.databaseName;
  }

  protected void setStatementArguments(final PreparedStatement statement, final List<?> arguments) throws SQLException {
    final int count = arguments.size();

    for (int idx = 0; idx < count; ++idx) {
      final Object argument = arguments.get(idx);
      final Class<?> argumentType = argument.getClass();

      if (argumentType == String.class) {
        statement.setString(idx + 1, (String) argument);
      } else if (argumentType == Integer.class) {
        statement.setInt(idx + 1, (Integer) argument);
      } else if (argumentType == Long.class) {
        statement.setLong(idx + 1, (Long) argument);
      } else if (argumentType == Boolean.class) {
        statement.setBoolean(idx + 1, (Boolean) argument);
      } else if (argumentType == Date.class) {
        java.sql.Date sqlDate = new java.sql.Date(((Date) argument).getTime());
        statement.setDate(idx + 1, sqlDate);
      } else if (argumentType == Double.class) {
        statement.setDouble(idx + 1, (Double) argument);
      } else if (argumentType == Float.class) {
        statement.setFloat(idx + 1, (Float) argument);
      }
    }
  }

  /**
   * Answer whether the table with {@code tableName} exists.
   * Uses {@code catalogName()} to for the JDBC {@code getTables()} call.
   * Override {@code catalogName()} to change the default behavior.
   * 
   * <p>NOTE: Override for subclass specialized implementation
   * 
   * @param tableName the String name of the table to check for existence
   * @return boolean
   * @throws Exception
   */
  protected boolean tableExists(final String tableName) throws Exception {
    final DatabaseMetaData metadata = connection.getMetaData();
    try (final ResultSet resultSet = metadata.getTables(catalogName(), null, tableName, null)) {
      return resultSet.next();
    }
  }

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

  private void createEntryOffsetsTable() throws Exception {
    final String tableName = entryOffsetsTableName();
    if (!tableExists(tableName)) {
      try (final Statement statement = connection.createStatement()) {
        statement.executeUpdate(entryOffsetsTableCreateExpression());
        connection.commit();
      } catch (final Exception e) {
        throw new IllegalStateException("Cannot create table " + tableName + " because: " + e, e);
      }
    }
  }

  private void createStateStoreTable(final String tableName) throws Exception {
    final String sql = stateStoreTableCreateExpression(tableName);
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

    try {
      createEntryOffsetsTable();
    } catch (Exception e) {
      // assume table exists; could look at metadata
      logger.error("Could not create entry table because: " + e.getMessage(), e);
    }

    for (final String storeName : StateTypeStateStoreMap.allStoreNames()) {
      final String tableName = tableNameFor(storeName);
      try {
        logger.info("Checking for table: " + tableName);
        if (!tableExists(tableName)) {
          logger.info("Creating missing table: " + tableName);
          createStateStoreTable(tableName);
        }
      } catch (final Exception e) {
        // assume table exists; could look at metadata
        logger.error("Could not create " + tableName + " table because: " + e.getMessage(), e);
      }
    }
  }

  private <E> void prepareForAppend(final CachedStatement<T> cached, final Entry<E> entry) throws Exception {
    cached.preparedStatement.clearParameters();
    cached.preparedStatement.setString(1, entry.typeName());
    cached.preparedStatement.setInt(2, entry.typeVersion());
    if (format.isBinary()) {
      this.setBinaryObject(cached, 3, entry);
    } else if (format.isText()) {
      this.setTextObject(cached, 3, entry);
    }
    cached.preparedStatement.setString(4, entry.metadata().value);
    cached.preparedStatement.setString(5, entry.metadata().operation);
    cached.preparedStatement.setInt(6, entry.entryVersion());
  }

  private void prepareForBatchAppend(final CachedStatement<T> cached, final List<Entry<?>> entries) throws Exception {
    for (Entry<?> entry : entries) {
      prepareForAppend(cached, entry);
      cached.preparedStatement.addBatch();
    }

    cached.preparedStatement.clearParameters();
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

  private void prepareForBatchWrite(final CachedStatement<T> cached, final List<? extends State<?>> states) throws Exception {
    for (State<?> state : states) {
      prepareForWrite(cached, state);
      cached.preparedStatement.addBatch();
    }
  }

  private Tuple2<String, String> serialized(final Object object) {
    if (object != null) {
      return Tuple2.from(JsonSerialization.serialized(object), object.getClass().getName());
    }
    return Tuple2.from(null, null);
  }
}
