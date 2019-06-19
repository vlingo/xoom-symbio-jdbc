// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.Update;

import io.vlingo.actors.Logger;
import io.vlingo.actors.Stage;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.object.ObjectStoreReader;
import io.vlingo.symbio.store.object.PersistentEntry;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;
import io.vlingo.symbio.store.object.jdbc.JDBCObjectStoreDelegate;
import io.vlingo.symbio.store.object.jdbc.jdbi.UnitOfWork.AlwaysModifiedUnitOfWork;

/**
 * The {@code JDBCObjectStoreDelegate} for Jdbi.
 */
public class JdbiObjectStoreDelegate extends JDBCObjectStoreDelegate {
  private static final String BindListKey = "listArgValues";
  private static final UnitOfWork AlwaysModified = new AlwaysModifiedUnitOfWork();

  private final EntryAdapterProvider entryAdapterProvider;
  private final Handle handle;
  private final Logger logger;
  private final Map<Class<?>,PersistentObjectMapper> mappers;
  private final Map<Long,UnitOfWork> unitOfWorkRegistry;
  private long updateId;

  /**
   * Constructs my default state.
   * @param stage the Stage I use
   * @param configuration the Configuration used to configure my concrete subclasses
   */
  public JdbiObjectStoreDelegate(final Stage stage, final Configuration configuration) {
    super(configuration);
    this.handle = Jdbi.open(configuration.connection);
    this.mappers = new HashMap<>();
    this.unitOfWorkRegistry = new HashMap<>();
    this.updateId = 0;
    this.logger = stage.world().defaultLogger();
    this.entryAdapterProvider = EntryAdapterProvider.instance(stage.world());

    initialize();
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#close()
   */
  @Override
  public void close() {
    try {
      handle.close();
    } catch (Exception e) {
      logger.error("Close failed because: " + e.getMessage(), e);
    }
  }

  @Override
  public <T extends PersistentObject, E> void persist(final T persistentObject, final List<Source<E>> sources, final long updateId, final PersistResultInterest interest, final Object object) {

    final boolean create = ObjectStoreReader.isNoId(updateId);
    final UnitOfWork unitOfWork = unitOfWorkRegistry.getOrDefault(updateId, AlwaysModified);

    try {
      final int actual = handle.inTransaction(handle -> {
        int total = 0;
        total += persistEach(handle, unitOfWork, persistentObject, create, interest, object);
        appendEntries(sources);
        return total;
      });
      unitOfWorkRegistry.remove(updateId);
      interest.persistResultedIn(Success.of(Result.Success), persistentObject, 1, actual, object);
    } catch (Exception e) {
      // NOTE: UnitOfWork not removed in case retry; see intervalSignal() for timeout-based removal

      logger.error("Persist of: " + persistentObject + " failed because: " + e.getMessage(), e);

      interest.persistResultedIn(
              Failure.of(new StorageException(Result.Failure, e.getMessage(), e)),
              persistentObject, 1, 0,
              object);
    }
  }

  @Override
  public <T extends PersistentObject, E> void persistAll(final Collection<T> persistentObjects, final List<Source<E>> sources, final long updateId, final PersistResultInterest interest, final Object object) {
    final boolean create = ObjectStoreReader.isNoId(updateId);
    final UnitOfWork unitOfWork = unitOfWorkRegistry.getOrDefault(updateId, AlwaysModified);

    try {
      final int actual = handle.inTransaction(handle -> {
        int total = 0;
        for (final T each : persistentObjects) {
          total += persistEach(handle, unitOfWork, each, create, interest, object);
        }
        appendEntries(sources);
        return total;
      });
      unitOfWorkRegistry.remove(updateId);
      interest.persistResultedIn(Success.of(Result.Success), persistentObjects, persistentObjects.size(), actual, object);
    } catch (Exception e) {
      // NOTE: UnitOfWork not removed in case retry; see intervalSignal() for timeout-based removal

      logger.error("Persist all of: " + persistentObjects + " failed because: " + e.getMessage(), e);

      interest.persistResultedIn(
              Failure.of(new StorageException(Result.Failure, e.getMessage(), e)),
              persistentObjects, persistentObjects.size(), 0,
              object);
    }
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#queryAll(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
   */
  @Override
  @SuppressWarnings("unchecked")
  public void queryAll(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    final List<?> results;

    if (expression.isListQueryExpression()) {
      results = handle.createQuery(expression.query)
                      .bindList(BindListKey, expression.asListQueryExpression().parameters)
                      .mapTo(expression.type)
                      .list();
    } else if (expression.isMapQueryExpression()) {
      results = handle.createQuery(expression.query)
                      .bindMap(expression.asMapQueryExpression().parameters)
                      .mapTo(expression.type)
                      .list();
    } else {
      results = handle.createQuery(expression.query)
              .mapTo(expression.type)
              .list();
    }

    List<PersistentObject> resultsAsPersistentObjects = (List<PersistentObject>) results;
    interest.queryAllResultedIn(Success.of(Result.Success), queryMultiResults(resultsAsPersistentObjects, expression.mode), object);
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#queryObject(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
   */
  @Override
  public void queryObject(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    final Optional<?> result;

    if (expression.isListQueryExpression()) {
      result = handle.createQuery(expression.query)
                     .bindList(BindListKey, expression.asListQueryExpression().parameters)
                     .mapTo(expression.type)
                     .findFirst();
    } else if (expression.isMapQueryExpression()) {
      result = handle.createQuery(expression.query)
                     .bindMap(expression.asMapQueryExpression().parameters)
                     .mapTo(expression.type)
                     .findFirst();
    } else {
      result = handle.createQuery(expression.query)
              .mapTo(expression.type)
              .findFirst();
    }

    final PersistentObject presistentObject = result.isPresent() ? (PersistentObject) result.get() : null;

    interest.queryObjectResultedIn(Success.of(Result.Success), querySingleResult(presistentObject, expression.mode), object);
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#registerMapper(java.lang.Object)
   */
  @Override
  public void registerMapper(final PersistentObjectMapper mapper) {
    mappers.put(mapper.type(), mapper);
    handle.registerRowMapper((RowMapper<?>) mapper.queryMapper());
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

  private <E> void appendEntries(final List<Source<E>> sources) {
    final Collection<BaseEntry<?>> all = entryAdapterProvider.asEntries(sources);
    final JdbiPersistMapper mapper = mappers.get(Entry.class).persistMapper();
    for (final BaseEntry<?> entry : all) {
      final Update statement = handle.createUpdate(mapper.insertStatement);
      mapper.binder.apply(statement, new PersistentEntry(entry)).execute();
    }
  }

  private void initialize() {
    // It is strange to me, but the only way to support real atomic
    // transactions (vs each statement is a transaction) in Jdbi is
    // to set the connection to auto-commit true. This seems intuitively
    // backwards, but fact nonetheless.
    try {
      handle.getConnection().setAutoCommit(true);
    } catch (Exception e) {
      logger.error("The connection could not be set to auto-commit; transactional problems likely.", e);
    }
  }

  private <T extends PersistentObject> int persistEach(final Handle handle, final UnitOfWork unitOfWork, final T persistentObject, final boolean create, final PersistResultInterest interest, final Object object) {

    final PersistentObject typed = PersistentObject.from(persistentObject);

    if (unitOfWork.isModified(typed)) {
      final Class<?> type = persistentObject.getClass();

      final JdbiPersistMapper mapper = mappers.get(type).persistMapper();

      final Update statement = create ?
              handle.createUpdate(mapper.insertStatement) :
              handle.createUpdate(mapper.updateStatement);

      return mapper.binder.apply(statement, persistentObject).execute();
    }

    return 0;
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
