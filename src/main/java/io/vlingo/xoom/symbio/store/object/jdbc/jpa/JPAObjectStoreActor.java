// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.object.jdbc.jpa;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.actors.Definition;
import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.common.Completes;
import io.vlingo.xoom.common.Failure;
import io.vlingo.xoom.common.Success;
import io.vlingo.xoom.common.identity.IdentityGenerator;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.EntryAdapterProvider;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.Source;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.EntryReader;
import io.vlingo.xoom.symbio.store.QueryExpression;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl.DispatcherControlInstantiator;
import io.vlingo.xoom.symbio.store.dispatch.control.DispatcherControlActor;
import io.vlingo.xoom.symbio.store.object.ObjectStoreEntryReader;
import io.vlingo.xoom.symbio.store.object.StateObject;
import io.vlingo.xoom.symbio.store.object.StateSources;
import io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreEntryReaderActor;
import io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreEntryReaderActor.JDBCObjectStoreEntryReaderInstantiator;

/**
 * JPAObjectStoreActor
 */
public class JPAObjectStoreActor extends Actor implements JPAObjectStore {
  private final ConnectionProvider connectionProvider;
  private final DatabaseType databaseType;
  private final DispatcherControl dispatcherControl;
  private final List<Dispatcher<Dispatchable<Entry<String>, State<?>>>> dispatchers;
  private boolean closed;
  private final JPAObjectStoreDelegate delegate;
  private final EntryAdapterProvider entryAdapterProvider;
  private final Map<String,ObjectStoreEntryReader<?>> entryReaders;
  private final Logger logger;
  private final IdentityGenerator identityGenerator;

  /**
   * Construct my default state.
   * @param delegate the JPAObjectStoreDelegate
   * @param connectionProvider the ConnectionProvider
   * @param dispatcher the Dispatcher
   */
  public JPAObjectStoreActor(
          final JPAObjectStoreDelegate delegate,
          final ConnectionProvider connectionProvider,
          final Dispatcher<Dispatchable<Entry<String>, State<?>>> dispatcher) {
    this(delegate, connectionProvider, dispatcher, DefaultCheckConfirmationExpirationInterval, DefaultConfirmationExpiration);
  }

  /**
   * Construct my default state.
   * @param delegate the JPAObjectStoreDelegate
   * @param connectionProvider the ConnectionProvider
   * @param dispatchers the {@code List<Dispatcher<Dispatchable<Entry<String>, State<?>>>>}
   * @param checkConfirmationExpirationInterval the long confirmation expiration interval
   * @param confirmationExpiration the long confirmation expiration
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public JPAObjectStoreActor (
          final JPAObjectStoreDelegate delegate,
          final ConnectionProvider connectionProvider,
          final List<Dispatcher<Dispatchable<Entry<String>, State<?>>>> dispatchers,
          final long checkConfirmationExpirationInterval,
          final long confirmationExpiration) {
    this.delegate = delegate;
    this.connectionProvider = connectionProvider;
    this.dispatchers = dispatchers;
    this.entryReaders = new HashMap<>();
    this.closed = false;
    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.logger = stage().world().defaultLogger();
    this.identityGenerator = new IdentityGenerator.RandomIdentityGenerator();

    this.dispatcherControl = stage().actorFor(
            DispatcherControl.class,
            Definition.has(
                    DispatcherControlActor.class,
                    new DispatcherControlInstantiator(
                            //Get a copy of storage delegate to use other connection
                            dispatchers, delegate.copy(),
                            checkConfirmationExpirationInterval,
                            confirmationExpiration)));

    try (final Connection initConnection = connectionProvider.newConnection()) {
      try {
        this.databaseType = DatabaseType.databaseType(initConnection);
        initConnection.commit();
      } catch (Exception e) {
        initConnection.rollback();
        throw new RuntimeException("Failed to instantiate JPAObjectStoreActor because: " + e.getMessage(), e);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to instantiate (connection) JPAObjectStoreActor because: " + e.getMessage(), e);
    }
  }

  /**
   * Construct my default state.
   * @param delegate the JPAObjectStoreDelegate
   * @param connectionProvider the ConnectionProvider
   * @param dispatcher the Dispatcher
   * @param checkConfirmationExpirationInterval the long confirmation expiration interval
   * @param confirmationExpiration the long confirmation expiration
   */
  public JPAObjectStoreActor(
          final JPAObjectStoreDelegate delegate,
          final ConnectionProvider connectionProvider,
          final Dispatcher<Dispatchable<Entry<String>, State<?>>> dispatcher,
          final long checkConfirmationExpirationInterval,
          final long confirmationExpiration) {
    this(delegate, connectionProvider, Arrays.asList(dispatcher), checkConfirmationExpirationInterval, confirmationExpiration);
  }

  /* @see io.vlingo.xoom.symbio.store.object.ObjectStore#close() */
  @Override
  public void close() {
    if (!closed) {
      delegate.close();
      if ( this.dispatcherControl != null ) {
        this.dispatcherControl.stop();
      }
      this.closed = true;
    }
  }

  @Override
  public Completes<EntryReader<? extends Entry<?>>> entryReader(final String name) {
    ObjectStoreEntryReader<?> reader = entryReaders.get(name);
    if (reader == null) {
      reader = childActorFor(
          ObjectStoreEntryReader.class,
          Definition.has(JDBCObjectStoreEntryReaderActor.class, new JDBCObjectStoreEntryReaderInstantiator(databaseType, connectionProvider, name)));
      entryReaders.put(name, reader);
    }

    return completes().with(reader);
  }

  @Override
  public <T extends StateObject, E> void persist(StateSources<T, E> stateSources, Metadata metadata, long updateId, PersistResultInterest interest, Object object) {
    final List<Source<E>> sources = stateSources.sources();
    final T persistentObject = stateSources.stateObject();
    try {

      delegate.beginTransaction();

      final State<?> state = delegate.persist(persistentObject, updateId, metadata);

      final int entryVersion = (int) stateSources.stateObject().version();
      final List<Entry<String>> entries = entryAdapterProvider.asEntries(sources, entryVersion, metadata);
      delegate.persistEntries(entries);

      final Dispatchable<Entry<String>, State<?>> dispatchable = buildDispatchable(state, entries);
      delegate.persistDispatchable(dispatchable);

      delegate.completeTransaction();

      dispatchers.forEach(d -> d.dispatch(dispatchable));
      interest.persistResultedIn(Success.of(Result.Success), persistentObject, 1, 1, object);

    } catch (final StorageException e) {
      logger.error("Persist of: " + persistentObject + " failed because: " + e.getMessage(), e);
      delegate.failTransaction();
      interest.persistResultedIn(Failure.of(e), persistentObject, 1, 0, object);
    } catch (final Exception e) {
      logger.error("Persist of: " + persistentObject + " failed because: " + e.getMessage(), e);
      delegate.failTransaction();
      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), persistentObject,
              1, 0, object);
    }
  }

  @Override
  public <T extends StateObject, E> void persistAll(Collection<StateSources<T, E>> allStateSources, Metadata metadata, long updateId, PersistResultInterest interest, Object object) {
    final Collection<T> allPersistentObjects = new ArrayList<>();
    final List<Dispatchable<Entry<String>, State<?>>> allDispatchables = new ArrayList<>();
    try {
      delegate.beginTransaction();
      for (StateSources<T,E> stateSources : allStateSources) {
        final T persistentObject = stateSources.stateObject();
        final List<Source<E>> sources = stateSources.sources();

        final int entryVersion = (int) stateSources.stateObject().version();
        final List<Entry<String>> entries = entryAdapterProvider.asEntries(sources, entryVersion, metadata);
        delegate.persistEntries(entries);

        final State<?> state = delegate.persist(persistentObject, updateId, metadata);
        allPersistentObjects.add(persistentObject);

        final Dispatchable<Entry<String>, State<?>> dispatchable = buildDispatchable(state, entries);
        delegate.persistDispatchable(dispatchable);
        allDispatchables.add(dispatchable);
      }
      delegate.completeTransaction();

      allDispatchables.forEach(dispatchable -> dispatchers.forEach(d -> d.dispatch(dispatchable)));
      interest.persistResultedIn(Success.of(Result.Success), allPersistentObjects, allPersistentObjects.size(), allPersistentObjects.size(), object);

    } catch (final StorageException e) {
      logger.error("Persist all of: " + allPersistentObjects + " failed because: " + e.getMessage(), e);
      delegate.failTransaction();
      interest.persistResultedIn(Failure.of(e), allPersistentObjects, allPersistentObjects.size(), 0, object);
    } catch (final Exception e) {
      logger.error("Persist all of: " + allPersistentObjects + " failed because: " + e.getMessage(), e);
      delegate.failTransaction();

      interest.persistResultedIn(
        Failure.of(new StorageException(Result.Failure, e.getMessage(), e)),
        allPersistentObjects, allPersistentObjects.size(), 0, object);
    }
  }

  /*
   * @see
   * io.vlingo.xoom.symbio.store.object.ObjectStore#queryAll(io.vlingo.xoom.symbio.store.
   * object.QueryExpression,
   * io.vlingo.xoom.symbio.store.object.ObjectStore.QueryResultInterest,
   * java.lang.Object)
   */
  @Override
  public void queryAll(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    try {
      final QueryMultiResults results = delegate.queryAll(expression);
      interest.queryAllResultedIn(Success.of(Result.Success), results, object);
    } catch (final StorageException e) {
      logger.error("Query all failed because: " + e.getMessage(), e);
      interest.queryAllResultedIn(Failure.of(e), QueryMultiResults.of(null), object);
    }
  }

  /*
   * @see
   * io.vlingo.xoom.symbio.store.object.ObjectStore#queryObject(io.vlingo.xoom.symbio.store.
   * object.QueryExpression,
   * io.vlingo.xoom.symbio.store.object.ObjectStore.QueryResultInterest,
   * java.lang.Object)
   */
  @Override
  public void queryObject(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    try {
      final QuerySingleResult result = delegate.queryObject(expression);
      if (result.stateObject !=null ){
        interest.queryObjectResultedIn(Success.of(Result.Success), result, object);
      } else {
        interest.queryObjectResultedIn(Failure.of(new StorageException(Result.NotFound, "No object identified by expression: " + expression)), result, object);
      }
    } catch (final StorageException e){
      logger.error("Query all failed because: " + e.getMessage(), e);
      interest.queryObjectResultedIn(Failure.of(e), QuerySingleResult.of(null), object);
    }
  }

  @Override
  public <T extends StateObject> void remove(final T persistentObject, final long removeId, final PersistResultInterest interest, final Object object) {
    try {
      final int count = delegate.remove(persistentObject, removeId);
      interest.persistResultedIn(Success.of(Result.Success), persistentObject, 1, count, object);
    } catch (final StorageException e){
      logger.error("Remove failed because: " + e.getMessage(), e);
      interest.persistResultedIn(Failure.of(e), persistentObject,1, 0, object);
    } catch (final Exception e){
      logger.error("Remove failed because: " + e.getMessage(), e);
      interest.persistResultedIn(Failure.of(new StorageException(Result.Error, e.getMessage(), e)), persistentObject,
              1, 0, object);
    }
  }

  private Dispatchable<Entry<String>, State<?>> buildDispatchable(final State<?> state, final List<Entry<String>> entries){
    final String id = identityGenerator.generate().toString();
    return new Dispatchable<>(id, LocalDateTime.now(), state, entries);
  }
}
