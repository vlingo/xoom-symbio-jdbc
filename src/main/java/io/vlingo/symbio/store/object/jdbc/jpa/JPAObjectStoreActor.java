// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Definition;
import io.vlingo.actors.Logger;
import io.vlingo.common.Completes;
import io.vlingo.common.Failure;
import io.vlingo.common.Success;
import io.vlingo.common.identity.IdentityGenerator;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.EntryReader;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.dispatch.control.DispatcherControlActor;
import io.vlingo.symbio.store.object.ObjectStoreEntryReader;
import io.vlingo.symbio.store.object.QueryExpression;
import io.vlingo.symbio.store.object.StateObject;
import io.vlingo.symbio.store.object.jdbc.JDBCObjectStoreEntryReaderActor;

/**
 * JPAObjectStoreActor
 */
public class JPAObjectStoreActor extends Actor implements JPAObjectStore {
  private final ConnectionProvider connectionProvider;
  private final DispatcherControl dispatcherControl;
  private final Dispatcher<Dispatchable<Entry<String>, State<?>>> dispatcher;
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
    this(delegate, connectionProvider, dispatcher, 1000L, 1000L);
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
    this.delegate = delegate;
    this.connectionProvider = connectionProvider;
    this.dispatcher = dispatcher;
    this.entryReaders = new HashMap<>();
    this.closed = false;
    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.logger = stage().world().defaultLogger();
    this.identityGenerator = new IdentityGenerator.RandomIdentityGenerator();

    this.dispatcherControl = stage().actorFor(
            DispatcherControl.class,
            Definition.has(
                    DispatcherControlActor.class,
                    Definition.parameters(
                            //Get a copy of storage delegate to use other connection
                            dispatcher, delegate.copy(),
                            checkConfirmationExpirationInterval,
                            confirmationExpiration)));
  }

  /* @see io.vlingo.symbio.store.object.ObjectStore#close() */
  @Override
  public void close() {
    if (!closed) {
      delegate.close();
      if ( this.dispatcherControl != null ){
        this.dispatcherControl.stop();
      }
      this.closed = true;
    }
  }

  @Override
  public Completes<EntryReader<? extends Entry<?>>> entryReader(final String name) {
    ObjectStoreEntryReader<?> reader = entryReaders.get(name);
    if (reader == null) {
      final Connection connection = connectionProvider.connection();
      final DatabaseType databaseType = DatabaseType.databaseType(connection);

      reader = childActorFor(
              ObjectStoreEntryReader.class,
              Definition.has(JDBCObjectStoreEntryReaderActor.class, Definition.parameters(databaseType, connection, name)));
      entryReaders.put(name, reader);
    }
    return completes().with(reader);
  }

  @Override
  public <T extends StateObject, E> void persist(final T persistentObject, final List<Source<E>> sources, final Metadata metadata, final long updateId,
          final PersistResultInterest interest, final Object object) {

    try {
      final List<Entry<String>> entries = entryAdapterProvider.asEntries(sources, metadata);

      delegate.beginTransaction();
      final State<?> raw = delegate.persist(persistentObject, updateId, metadata);
      delegate.persistEntries(entries);

      final Dispatchable<Entry<String>, State<?>> dispatchable = buildDispatchable(raw, entries);

      delegate.persistDispatchable(dispatchable);

      delegate.completeTransaction();

      dispatcher.dispatch(dispatchable);

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
  public <T extends StateObject, E> void persistAll(final Collection<T> persistentObjects, final List<Source<E>> sources, final Metadata metadata,
          final long updateId, final PersistResultInterest interest, final Object object) {

    try {
      final List<Entry<String>> entries = entryAdapterProvider.asEntries(sources, metadata);
      final List<Dispatchable<Entry<String>, State<?>>> dispatchables = new ArrayList<>(persistentObjects.size());

      delegate.beginTransaction();

      final Collection<State<?>> states = delegate.persistAll(persistentObjects, updateId, metadata);
      states.forEach(state-> {
        dispatchables.add(buildDispatchable(state, entries));
      });

      delegate.persistEntries(entries);
      dispatchables.forEach(delegate::persistDispatchable);

      delegate.completeTransaction();

      //Dispatch after commit
      dispatchables.forEach(dispatcher::dispatch);

      interest.persistResultedIn(Success.of(Result.Success), persistentObjects, persistentObjects.size(), persistentObjects.size(), object);
    } catch (final StorageException e) {
      logger.error("Persist all of: " + persistentObjects + " failed because: " + e.getMessage(), e);
      delegate.failTransaction();
      interest.persistResultedIn(Failure.of(e), persistentObjects, persistentObjects.size(), 0, object);
    } catch (final Exception e) {
      logger.error("Persist all of: " + persistentObjects + " failed because: " + e.getMessage(), e);
      delegate.failTransaction();

      interest.persistResultedIn(
              Failure.of(new StorageException(Result.Failure, e.getMessage(), e)),
              persistentObjects, persistentObjects.size(), 0, object);
    }
  }

  /*
   * @see
   * io.vlingo.symbio.store.object.ObjectStore#queryAll(io.vlingo.symbio.store.
   * object.QueryExpression,
   * io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest,
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
   * io.vlingo.symbio.store.object.ObjectStore#queryObject(io.vlingo.symbio.store.
   * object.QueryExpression,
   * io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest,
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
