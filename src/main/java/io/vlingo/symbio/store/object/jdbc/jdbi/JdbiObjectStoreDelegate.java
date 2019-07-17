// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import io.vlingo.actors.Logger;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.object.ObjectStoreReader;
import io.vlingo.symbio.store.object.ObjectStoreReader.QueryMode;
import io.vlingo.symbio.store.object.ObjectStoreReader.QueryMultiResults;
import io.vlingo.symbio.store.object.ObjectStoreReader.QuerySingleResult;
import io.vlingo.symbio.store.object.PersistentEntry;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;
import io.vlingo.symbio.store.object.jdbc.JDBCObjectStoreDelegate;
import io.vlingo.symbio.store.object.jdbc.jdbi.UnitOfWork.AlwaysModifiedUnitOfWork;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.generic.GenericType;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.result.ResultBearing;
import org.jdbi.v3.core.statement.Update;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@code JDBCObjectStoreDelegate} for Jdbi.
 */
public class JdbiObjectStoreDelegate extends JDBCObjectStoreDelegate {
  private static final String BindListKey = "listArgValues";
  private static final UnitOfWork AlwaysModified = new AlwaysModifiedUnitOfWork();

  private final StateAdapterProvider stateAdapterProvider;
  private final Handle handle;
  private final Logger logger;
  private final Map<Class<?>, PersistentObjectMapper> mappers;
  private final Map<Long, UnitOfWork> unitOfWorkRegistry;
  private long updateId;
  private final QueryExpression unconfirmedDispatchablesExpression;

  /**
   * Constructs my default state.
   *
   * @param configuration                      the Configuration used to configure my concrete subclasses
   * @param stateAdapterProvider               {@code StateAdapterProvider} used get raw {@code State<?>} from {@code PersistentObject}
   * @param unconfirmedDispatchablesExpression the query expression to use for getting unconfirmed dispatchables
   * @param mappers                            collection of {@code PersistentObjectMapper} to be registered
   * @param logger                             the instance of {@link Logger} to be used
   */
  public JdbiObjectStoreDelegate(final Configuration configuration, final StateAdapterProvider stateAdapterProvider,
          final QueryExpression unconfirmedDispatchablesExpression, final Collection<PersistentObjectMapper> mappers, final Logger logger) {
    super(configuration);
    this.handle = Jdbi.open(configuration.connection);
    this.stateAdapterProvider = stateAdapterProvider;
    this.unconfirmedDispatchablesExpression = unconfirmedDispatchablesExpression;
    this.mappers = new HashMap<>();
    this.unitOfWorkRegistry = new ConcurrentHashMap<>();
    this.updateId = 0;
    this.logger = logger;
    initialize();

    mappers.forEach(mapper -> {
      this.mappers.put(mapper.type(), mapper);
      this.handle.registerRowMapper((RowMapper<?>) mapper.queryMapper());
    });
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#close()
   */
  @Override
  public void close() {
    try {
      handle.close();
    } catch (final Exception e) {
      logger.error("Close failed because: " + e.getMessage(), e);
    }
  }

  @Override
  public JDBCObjectStoreDelegate copy() {
    try {
      return new JdbiObjectStoreDelegate(Configuration.cloneOf(configuration), stateAdapterProvider, this.unconfirmedDispatchablesExpression, mappers.values(),
              logger);
    } catch (final Exception e) {
      final String message = "Copy of JDBCObjectStoreDelegate failed because: " + e.getMessage();
      logger.error(message, e);
      throw new IllegalStateException(message, e);
    }
  }

  @Override
  public void beginTransaction() {
    handle.begin();
  }

  @Override
  public void completeTransaction() {
    handle.commit();
  }

  @Override
  public void failTransaction() {
    handle.rollback();
  }

  @Override
  public <T extends PersistentObject> Collection<State<?>> persistAll(final Collection<T> persistentObjects, final long updateId, final Metadata metadata)
          throws StorageException {
    final boolean create = ObjectStoreReader.isNoId(updateId);
    final UnitOfWork unitOfWork = unitOfWorkRegistry.getOrDefault(updateId, AlwaysModified);
    final List<State<?>> states = new ArrayList<>();

    for (final T each : persistentObjects) {
      final State<?> state = getRawState(metadata, each);
      persistEach(handle, unitOfWork, each, create);
      states.add(state);
    }

    unitOfWorkRegistry.remove(updateId);
    return states;
  }

  @Override
  public <T extends PersistentObject> State<?> persist(final T persistentObject, final long updateId, final Metadata metadata) throws StorageException {
    final boolean create = ObjectStoreReader.isNoId(updateId);
    final UnitOfWork unitOfWork = unitOfWorkRegistry.getOrDefault(updateId, AlwaysModified);
    final State<?> state = getRawState(metadata, persistentObject);

    persistEach(handle, unitOfWork, persistentObject, create);
    
    unitOfWorkRegistry.remove(updateId);
    return state;
  }

  private <T extends PersistentObject> State<?> getRawState(final Metadata metadata, final T detachedEntity) {
    return this.stateAdapterProvider.asRaw(String.valueOf(detachedEntity.persistenceId()), detachedEntity, 1, metadata);
  }

  @Override
  public void persistEntries(final Collection<Entry<?>> entries) throws StorageException {
    final JdbiPersistMapper mapper = mappers.get(Entry.class).persistMapper();
    for (final Entry<?> entry : entries) {
      final Update statement = handle.createUpdate(mapper.insertStatement);
      final ResultBearing resultBearing = mapper.binder.apply(statement, new PersistentEntry(entry)).executeAndReturnGeneratedKeys();
      final Object id = resultBearing.mapToMap().findOnly().get("e_id");
      ((BaseEntry<?>) entry).__internal__setId(id.toString());
    }
  }

  @Override
  public void persistDispatchable(final Dispatchable<Entry<?>, State<?>> dispatchable) throws StorageException {
    final JdbiPersistMapper mapper = mappers.get(dispatchable.getClass()).persistMapper();
    final Update statement = handle.createUpdate(mapper.insertStatement);
    mapper.binder.apply(statement, new PersistentDispatchable(configuration.originatorId, dispatchable)).execute();
  }

  @Override
  public QueryMultiResults queryAll(final QueryExpression expression) throws StorageException {
    final List<?> results;

    if (expression.isListQueryExpression()) {
      results = handle.createQuery(expression.query).bindList(BindListKey, expression.asListQueryExpression().parameters).mapTo(expression.type).list();
    } else if (expression.isMapQueryExpression()) {
      results = handle.createQuery(expression.query).bindMap(expression.asMapQueryExpression().parameters).mapTo(expression.type).list();
    } else {
      results = handle.createQuery(expression.query).mapTo(expression.type).list();
    }

    return queryMultiResults(results, expression.mode);
  }

  @Override
  public QuerySingleResult queryObject(final QueryExpression expression) throws StorageException {
    final Optional<?> result;

    if (expression.isListQueryExpression()) {
      result = handle.createQuery(expression.query).bindList(BindListKey, expression.asListQueryExpression().parameters).mapTo(expression.type).findFirst();
    } else if (expression.isMapQueryExpression()) {
      result = handle.createQuery(expression.query).bindMap(expression.asMapQueryExpression().parameters).mapTo(expression.type).findFirst();
    } else {
      result = handle.createQuery(expression.query).mapTo(expression.type).findFirst();
    }
    
    return querySingleResult(result.orElse(null), expression.mode);
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#registerMapper(java.lang.Object)
   */
  @Override
  public void registerMapper(final PersistentObjectMapper mapper) {
    //not to be used
  }

  /*
   * @see io.vlingo.symbio.store.object.jdbc.JDBCObjectStoreDelegate#timeoutCheck()
   */
  @Override
  public void timeoutCheck() {
    final long timeoutTime = System.currentTimeMillis() - configuration.transactionTimeoutMillis;
    final List<UnitOfWork> unitOfWorkList = new ArrayList<>(unitOfWorkRegistry.values());
    for (final UnitOfWork unitOfWork : unitOfWorkList) {
      if (unitOfWork.hasTimedOut(timeoutTime)) {
        unitOfWorkRegistry.remove(unitOfWork.unitOfWorkId);
      }
    }
  }

  @Override
  public Collection<Dispatchable<Entry<?>, State<?>>> allUnconfirmedDispatchableStates() {
    return handle.createQuery(unconfirmedDispatchablesExpression.query)
            .mapTo(new GenericType<Dispatchable<Entry<?>, State<?>>>() {})
            .list();
  }

  @Override
  public void confirmDispatched(final String dispatchId) {
    final JdbiPersistMapper mapper = mappers.get(Dispatchable.class).persistMapper();
    handle.createUpdate(mapper.updateStatement).bind("id", dispatchId).execute();
  }

  @Override
  public void stop() {
    this.close();
  }

  private void initialize() {
    // It is strange to me, but the only way to support real atomic
    // transactions (vs each statement is a transaction) in Jdbi is
    // to set the connection to auto-commit true. This seems intuitively
    // backwards, but fact nonetheless.
    try {
      handle.getConnection().setAutoCommit(true);
    } catch (final Exception e) {
      logger.error("The connection could not be set to auto-commit; transactional problems likely.", e);
    }
  }

  private <T extends PersistentObject> int persistEach(final Handle handle, final UnitOfWork unitOfWork, final T persistentObject, final boolean create) {

    final PersistentObject typed = PersistentObject.from(persistentObject);

    if (unitOfWork.isModified(typed)) {
      final Class<?> type = persistentObject.getClass();

      final JdbiPersistMapper mapper = mappers.get(type).persistMapper();

      final Update statement = create ? handle.createUpdate(mapper.insertStatement) : handle.createUpdate(mapper.updateStatement);

      return mapper.binder.apply(statement, persistentObject).execute();
    }

    return 1;
  }

  private QueryMultiResults queryMultiResults(final List<?> presistentObjects, final QueryMode mode) {
    if (mode.isReadUpdate() && !presistentObjects.isEmpty()) {
      return QueryMultiResults.of(presistentObjects, registerUnitOfWork(presistentObjects));
    }
    return QueryMultiResults.of(presistentObjects);
  }

  private QuerySingleResult querySingleResult(final Object presistentObject, final QueryMode mode) {
    if (mode.isReadUpdate() && presistentObject != null) {
      return QuerySingleResult.of(presistentObject, registerUnitOfWork(presistentObject));
    }
    return QuerySingleResult.of(presistentObject);
  }

  private long registerUnitOfWork(final Object presistentObject) {
    final long unitOfWorkId = ++updateId;

    unitOfWorkRegistry.put(unitOfWorkId, UnitOfWork.acquireFor(unitOfWorkId, presistentObject));

    return unitOfWorkId;
  }

  private long registerUnitOfWork(final List<?> presistentObjects) {
    final long unitOfWorkId = ++updateId;

    unitOfWorkRegistry.put(unitOfWorkId, UnitOfWork.acquireFor(unitOfWorkId, presistentObjects));

    return unitOfWorkId;
  }
}
