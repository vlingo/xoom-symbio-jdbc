// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jdbi;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.SqlStatement;

import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.symbio.BaseEntry.TextEntry;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.StateAdapterProvider;
import io.vlingo.xoom.symbio.store.ListQueryExpression;
import io.vlingo.xoom.symbio.store.QueryExpression;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.object.ObjectStore;
import io.vlingo.xoom.symbio.store.object.StateObjectMapper;
import io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreActor;
import io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreEntryJournalQueries;

/**
 * Defines a protocol for using Jdbi over a given database.
 */
public abstract class JdbiOnDatabase {
  public final Configuration configuration;
  private final DatabaseType databaseType;
  public final Handle handle;

  private final JDBCObjectStoreEntryJournalQueries queries;

  private ObjectStore objectStore;

  public static JdbiOnDatabase openUsing(final Configuration configuration) {
    try (final Connection initConnection = configuration.connectionProvider.newConnection()) {
      try {
        final DatabaseType databaseType = DatabaseType.databaseType(initConnection);
        switch (databaseType) {
          case HSQLDB:
            initConnection.commit();
            return JdbiOnHSQLDB.openUsing(configuration);
          case MySQL:
          case MariaDB:
            initConnection.commit();
            return JdbiOnMySQL.openUsing(configuration);
          case SQLServer:
          case Vitess:
          case Oracle:
            break;
          case Postgres:
            initConnection.commit();
            return JdbiOnPostgres.openUsing(configuration);
          case YugaByte:
            initConnection.commit();
            return JdbiOnYugaByte.openUsing(configuration);
        }

        initConnection.commit();
        throw new IllegalArgumentException("Database currently not supported: " + databaseType.name());
      } catch (Exception e) {
        initConnection.rollback();
        throw new RuntimeException("Failed to lookup database type because: " + e.getMessage(), e);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to lookup (connection) database type because: " + e.getMessage(), e);
    }
  }

//    /**
//     * Answer my {@code Configuration} instance.
//     * @return Configuration
//     */
//    public Configuration configuration() {
//        return configuration;
//    }

  /**
   * Close this resource.
   */
  public void close() {
    handle.close();
  }

  /**
   * Create all common tables.
   *
   * @throws SQLException when creation fails
   */
  public void createCommonTables(final Connection connection) throws SQLException {
    queries.createCommonTables(connection);
  }

  /**
   * Creates the table to store {@code Dispatchable} objects.
   *
   * @throws SQLException when creation fails
   */
  public void createDispatchableTable(final Connection connection) throws SQLException {
    queries.createDispatchableTable(connection);
  }

  /**
   * Creates the table used to store journal {@code Entry} objects.
   *
   * @throws SQLException when creation fails
   */
  public void createTextEntryJournalTable(final Connection connection) throws SQLException {
    queries.createTextEntryJournalTable(connection);
  }

  /**
   * Creates the table used to store the current offsets of entry readers.
   *
   * @throws SQLException when creation fails
   */
  public void createTextEntryJournalReaderOffsetsTable(final Connection connection) throws SQLException {
    queries.createTextEntryJournalReaderOffsetsTable(connection);
  }

  /**
   * Answer my {@code JdbiPersistMapper} for the current entry offset.
   *
   * @param placeholders the String[] of parameter place holders
   * @return JdbiPersistMapper
   */
  public abstract JdbiPersistMapper currentEntryOffsetMapper(final String[] placeholders);

  /**
   * Answer my {@code DatabaseType}.
   *
   * @return DatabaseType
   */
  public DatabaseType databaseType() {
    return databaseType;
  }

  /**
   * Answer my {@code Handle}.
   *
   * @return Handle
   */
  public Handle handle() {
    return handle;
  }

  /**
   * Answer whether if this resource is closed.
   *
   * @return
   */
  public boolean isClosed() {
    return handle.isClosed();
  }

  /**
   * Answer the {@code ObjectStore} instance for the host database.
   *
   * @param world       the World in which the ObjectStore implementing Actor is created
   * @param dispatchers the {@code List<Dispatcher<Dispatchable<TextEntry, State.TextState>>>} used by the ObjectStore
   * @param mappers     the Collection of PersistentObjectMapper for service/application specific tables
   * @return ObjectStore
   */
  public ObjectStore objectStore(
      final World world,
      final List<Dispatcher<Dispatchable<TextEntry, State.TextState>>> dispatchers,
      final Collection<StateObjectMapper> mappers) {
    if (objectStore == null) {
      final List<StateObjectMapper> objectMappers = new ArrayList<>(mappers);
      objectMappers.add(textEntryPersistentObjectMapper());
      objectMappers.add(dispatchableMapping());

      final StateAdapterProvider stateAdapterProvider = StateAdapterProvider.instance(world);

      final JdbiObjectStoreDelegate delegate = new JdbiObjectStoreDelegate(configuration, stateAdapterProvider, unconfirmedDispatchablesQueryExpression(), objectMappers, world.defaultLogger());

      objectStore = world.actorFor(ObjectStore.class, JDBCObjectStoreActor.class, delegate, dispatchers);
    }

    return objectStore;
  }

  /**
   * Answer the {@code ObjectStore} instance for the host database.
   *
   * @param world      the World in which the ObjectStore implementing Actor is created
   * @param dispatcher the Dispatcher used by the ObjectStore
   * @param mappers    the Collection of PersistentObjectMapper for service/application specific tables
   * @return ObjectStore
   */
  public ObjectStore objectStore(
      final World world,
      final Dispatcher<Dispatchable<TextEntry, State.TextState>> dispatcher,
      final Collection<StateObjectMapper> mappers) {
    return objectStore(world, Arrays.asList(dispatcher), mappers);
  }

  /**
   * Answer the {@code ObjectStore}, which should be used only following
   * {@link JdbiOnDatabase#objectStore(World, Dispatcher, Collection)}.
   *
   * @return ObjectStore
   */
  public ObjectStore objectStore() {
    return objectStore;
  }

  /**
   * Answer the {@code QueryExpression} for a single {@code Entry} instance.
   *
   * @param id the long identity to select (possibly greater than this id)
   * @return QueryExpression
   */
  public QueryExpression queryEntry(final long id) {
    return QueryExpression.using(Entry.class, queries.entryQuery(id));
  }

  /**
   * Answer the {@code QueryExpression} for multiple {@code Entry} instances.
   *
   * @param id    the long identity to begin selection (possibly greater than this id)
   * @param count the int Entry instance limit
   * @return QueryExpression
   */
  public QueryExpression queryEntries(final long id, final int count) {
    return ListQueryExpression.using(Entry.class, queries.entriesQuery(id, count));
  }

  /**
   * Answer the {@code QueryExpression} for multiple {@code Entry} instances based on ids.
   *
   * @param ids identities to be selected
   * @return QueryExpression
   */
  public QueryExpression queryEntries(List<Long> ids) {
    return ListQueryExpression.using(Entry.class, queries.entriesQuery(ids));
  }

  /**
   * Answer the {@code QueryExpression} for the id of the most recently inserted {@code Entry}.
   *
   * @return QueryExpression
   */
  public QueryExpression queryLastEntryId() {
    return QueryExpression.using(Long.class, queries.lastEntryIdQuery());
  }

  /**
   * Answer the {@code QueryExpression} for determining the size,
   * as in total number of entries.
   *
   * @return QueryExpression
   */
  public QueryExpression querySize() {
    return QueryExpression.using(Long.class, queries.sizeQuery());
  }

  /**
   * Answer my {@code TextDispatchablesMapper}
   *
   * @return TextDispatchablesMapper
   */
  public TextDispatchablesMapper textDispatchablesMapper() {
    return new TextDispatchablesMapper();
  }

  /**
   * Answer my {@code TextEntryMapper}
   *
   * @return TextEntryMapper
   */
  public TextEntryMapper textEntryMapper() {
    return new TextEntryMapper();
  }

  /**
   * Answer my {@code PersistentObjectMapper} for {@code Entry<String>} instances.
   *
   * @return PersistentObjectMapper
   */
  public StateObjectMapper textEntryPersistentObjectMapper() {
    final StateObjectMapper persistentObjectMapper =
        StateObjectMapper.with(
            Entry.class,
            JdbiPersistMapper.with(
                queries.insertEntriesQuery(
                    new String[]{
                        ":entry.type", ":entry.typeVersion", ":entry.entryData",
                        ":entry.metadata.value", ":entry.metadata.operation", ":entry.entryVersion"}),
                "(unused)",
                (update, object) -> update.bindMethods(object)),
            textEntryMapper());

    return persistentObjectMapper;
  }

  protected JdbiOnDatabase(final Configuration configuration, final DatabaseType databaseType) {
    this.configuration = configuration;
    this.databaseType = databaseType;
    this.handle = Jdbi.open(configuration.connectionProvider.dataSource());
    this.queries = JDBCObjectStoreEntryJournalQueries.using(databaseType);
  }

  private StateObjectMapper dispatchableMapping() {
    return StateObjectMapper.with(
        Dispatchable.class,
        JdbiPersistMapper.with(
            queries.insertDispatchableQuery(
                new String[]{
                    ":createdOn", ":originatorId", ":id", ":state.id", ":state.type",
                    ":state.typeVersion", ":state.data", ":state.dataVersion", ":state.metadata", ":entries"}),
            queries.deleteDispatchableQuery(":id"),
            SqlStatement::bindMethods),
        textDispatchablesMapper());
  }

  private QueryExpression unconfirmedDispatchablesQueryExpression() {
    return new QueryExpression(
        Dispatchable.class,
        queries.unconfirmedDispatchablesQuery(configuration.originatorId)
    );
  }
}
