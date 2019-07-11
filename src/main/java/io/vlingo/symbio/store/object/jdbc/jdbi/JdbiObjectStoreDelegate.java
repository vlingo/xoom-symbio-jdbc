// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import io.vlingo.actors.Logger;
import io.vlingo.common.Success;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.object.ObjectStoreReader;
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
   * @param unconfirmedDispatchablesExpression the query expression to use for getting unconfirmed dispatchables
   * @param mappers collection of {@code PersistentObjectMapper} to be registered
   * @param logger the instance of {@link Logger} to be used
   */
  public JdbiObjectStoreDelegate(final Configuration configuration, final QueryExpression unconfirmedDispatchablesExpression,
          final Collection<PersistentObjectMapper> mappers, final Logger logger) {
    super(configuration);
    this.handle = Jdbi.open(configuration.connection);
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
  public void beginWrite() {

  }

  @Override
  public void complete() {

  }

  @Override
  public void fail() {

  }

  @Override
  public JDBCObjectStoreDelegate copy() {
    try {
      return new JdbiObjectStoreDelegate(Configuration.cloneOf(configuration), this.unconfirmedDispatchablesExpression, mappers.values(), logger);
    } catch (final Exception e) {
      final String message = "Copy of JDBCObjectStoreDelegate failed because: " + e.getMessage();
      logger.error(message, e);
      throw new IllegalStateException(message, e);
    }
  }

  @Override
  public <T extends PersistentObject> void persist(final T persistentObject, final long updateId, final List<Entry<?>> entries,
          final Dispatchable<Entry<?>, State<?>> dispatchable) {
    final boolean create = ObjectStoreReader.isNoId(updateId);
    final UnitOfWork unitOfWork = unitOfWorkRegistry.getOrDefault(updateId, AlwaysModified);

    handle.inTransaction(handle -> {
      int total = 0;
      total += persistEach(handle, unitOfWork, persistentObject, create);
      appendEntries(entries);
      appendDispatchable(dispatchable);
      return total;
    });
    unitOfWorkRegistry.remove(updateId);
  }

  @Override
  public <T extends PersistentObject> void persistAll(final Collection<T> persistentObjects, final long updateId, final List<Entry<?>> entries,
          final Collection<Dispatchable<Entry<?>, State<?>>> dispatchable) {
    final boolean create = ObjectStoreReader.isNoId(updateId);
    final UnitOfWork unitOfWork = unitOfWorkRegistry.getOrDefault(updateId, AlwaysModified);

    handle.inTransaction(handle -> {
      int total = 0;
      for (final T each : persistentObjects) {
        total += persistEach(handle, unitOfWork, each, create);
      }
      appendEntries(entries);
      for (final Dispatchable<Entry<?>, State<?>> stateDispatchable : dispatchable) {
        appendDispatchable(stateDispatchable);
      }
      return total;
    });
    unitOfWorkRegistry.remove(updateId);
  }

  @Override
  public <T extends PersistentObject, E> void persist(final T persistentObject, final List<Source<E>> sources, final Metadata metadata, final long updateId,
          final PersistResultInterest interest, final Object object) {
    //not to be used
  }

  @Override
  public <T extends PersistentObject, E> void persistAll(final Collection<T> persistentObjects, final List<Source<E>> sources, final Metadata metadata,
          final long updateId, final PersistResultInterest interest, final Object object) {
    //not to be used
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#queryAll(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
   */
  @Override
  @SuppressWarnings("unchecked")
  public void queryAll(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    final List<?> results;

    if (expression.isListQueryExpression()) {
      results = handle.createQuery(expression.query).bindList(BindListKey, expression.asListQueryExpression().parameters).mapTo(expression.type).list();
    } else if (expression.isMapQueryExpression()) {
      results = handle.createQuery(expression.query).bindMap(expression.asMapQueryExpression().parameters).mapTo(expression.type).list();
    } else {
      results = handle.createQuery(expression.query).mapTo(expression.type).list();
    }

    final List<PersistentObject> resultsAsPersistentObjects = (List<PersistentObject>) results;
    interest.queryAllResultedIn(Success.of(Result.Success), queryMultiResults(resultsAsPersistentObjects, expression.mode), object);
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#queryObject(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
   */
  @Override
  public void queryObject(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    final Optional<?> result;

    if (expression.isListQueryExpression()) {
      result = handle.createQuery(expression.query).bindList(BindListKey, expression.asListQueryExpression().parameters).mapTo(expression.type).findFirst();
    } else if (expression.isMapQueryExpression()) {
      result = handle.createQuery(expression.query).bindMap(expression.asMapQueryExpression().parameters).mapTo(expression.type).findFirst();
    } else {
      result = handle.createQuery(expression.query).mapTo(expression.type).findFirst();
    }

    final PersistentObject presistentObject = (PersistentObject) result.orElse(null);

    interest.queryObjectResultedIn(Success.of(Result.Success), querySingleResult(presistentObject, expression.mode), object);
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

  private void appendEntries(final Collection<Entry<?>> all) {
    final JdbiPersistMapper mapper = mappers.get(Entry.class).persistMapper();
    for (final Entry<?> entry : all) {
      final Update statement = handle.createUpdate(mapper.insertStatement);
      final ResultBearing resultBearing = mapper.binder.apply(statement, new PersistentEntry(entry)).executeAndReturnGeneratedKeys();
      final Object id = resultBearing.mapToMap().findOnly().get("e_id");
      ((BaseEntry<?>) entry).__internal__setId(id.toString());
    }
  }

  private void appendDispatchable(final Dispatchable<Entry<?>, State<?>> dispatchable) {
    final JdbiPersistMapper mapper = mappers.get(dispatchable.getClass()).persistMapper();
    final Update statement = handle.createUpdate(mapper.insertStatement);
    mapper.binder.apply(statement, new PersistentDispatchable(configuration.originatorId, dispatchable)).execute();
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

  private <T extends PersistentObject> QueryMultiResults queryMultiResults(final List<T> presistentObjects, final QueryMode mode) {
    if (mode.isReadUpdate() && !presistentObjects.isEmpty()) {
      return QueryMultiResults.of(presistentObjects, registerUnitOfWork(presistentObjects));
    }
    return QueryMultiResults.of(presistentObjects);
  }

  private <T extends PersistentObject> QuerySingleResult querySingleResult(final T presistentObject, final QueryMode mode) {
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
