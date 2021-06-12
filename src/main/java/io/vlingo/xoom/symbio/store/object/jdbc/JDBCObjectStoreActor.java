// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc;

import java.sql.Connection;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vlingo.xoom.actors.Actor;
import io.vlingo.xoom.actors.ActorInstantiator;
import io.vlingo.xoom.actors.Address;
import io.vlingo.xoom.actors.Definition;
import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.common.Completes;
import io.vlingo.xoom.common.Failure;
import io.vlingo.xoom.common.Scheduled;
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
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl.DispatcherControlInstantiator;
import io.vlingo.xoom.symbio.store.dispatch.control.DispatcherControlActor;
import io.vlingo.xoom.symbio.store.object.ObjectStore;
import io.vlingo.xoom.symbio.store.object.ObjectStoreEntryReader;
import io.vlingo.xoom.symbio.store.object.StateObject;
import io.vlingo.xoom.symbio.store.object.StateSources;
import io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreEntryReaderActor.JDBCObjectStoreEntryReaderInstantiator;
import io.vlingo.xoom.symbio.store.object.jdbc.jdbi.JdbiObjectStoreEntryReaderActor;
import io.vlingo.xoom.symbio.store.object.jdbc.jdbi.JdbiObjectStoreEntryReaderActor.JdbiObjectStoreEntryReaderInstantiator;
import io.vlingo.xoom.symbio.store.object.jdbc.jdbi.JdbiOnDatabase;

/**
 * The actor implementing the {@code ObjectStore} protocol in behalf of
 * any number of {@code JDBCObjectStoreDelegate} types.
 */
public class JDBCObjectStoreActor extends Actor implements ObjectStore, Scheduled<Object> {
  private final DispatcherControl dispatcherControl;
  private boolean closed;
  private final JDBCObjectStoreDelegate delegate;
  private final List<Dispatcher<Dispatchable<Entry<?>, State<?>>>> dispatchers;
  private final Map<String,ObjectStoreEntryReader<?>> entryReaders;
  private final Logger logger;
  private final EntryAdapterProvider entryAdapterProvider;
  private final IdentityGenerator identityGenerator;

  public JDBCObjectStoreActor(final JDBCObjectStoreDelegate delegate, final List<Dispatcher<Dispatchable<Entry<?>, State<?>>>> dispatchers) {
     this(delegate, dispatchers, DefaultCheckConfirmationExpirationInterval, DefaultConfirmationExpiration);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public JDBCObjectStoreActor(
          final JDBCObjectStoreDelegate delegate,
          final List<Dispatcher<Dispatchable<Entry<?>, State<?>>>> dispatchers,
          final long checkConfirmationExpirationInterval,
          final long confirmationExpiration) {
    this.delegate = delegate;
    this.dispatchers = dispatchers;
    this.closed = false;
    this.logger = stage().world().defaultLogger();
    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.identityGenerator = new IdentityGenerator.RandomIdentityGenerator();
    this.entryReaders = new HashMap<>();

    final long timeout = delegate.configuration.transactionTimeoutMillis;
    stage().scheduler().schedule(selfAs(Scheduled.class), null, 5, timeout);

    this.dispatcherControl = stage().actorFor(
            DispatcherControl.class,
            Definition.has(
                    DispatcherControlActor.class,
                    new DispatcherControlInstantiator(
                            //Get a copy of storage delegate to use other connection
                            dispatchers, delegate.copy(),
                            checkConfirmationExpirationInterval,
                            confirmationExpiration)));
  }

  /**
   * @see io.vlingo.xoom.symbio.store.object.ObjectStore#close()
   */
  @Override
  public void close() {
    if (!closed) {
      delegate.close();
      if ( this.dispatcherControl != null ){
        this.dispatcherControl.stop();
      }
      closed = true;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Completes<EntryReader<? extends Entry<?>>> entryReader(final String name) {
    ObjectStoreEntryReader<? extends Entry<?>> entryReader = entryReaders.get(name);
    if (entryReader == null) {
      final Configuration clonedConfiguration = Configuration.cloneOf(delegate.configuration); // is clone still necessary?
      final Address address = stage().world().addressFactory().uniquePrefixedWith("objectStoreEntryReader-" + name);
      final Class<? extends Actor> actorType;
      ActorInstantiator<?> instantiator = null;

      switch (delegate.type()) {
      case Jdbi:
        actorType = JdbiObjectStoreEntryReaderActor.class;
        instantiator = new JdbiObjectStoreEntryReaderInstantiator(JdbiOnDatabase.openUsing(clonedConfiguration), delegate.registeredMappers(), name);
        break;
      case JDBC:
      case JPA:
        actorType = JDBCObjectStoreEntryReaderActor.class;
        try (final Connection connection = clonedConfiguration.connectionProvider.newConnection()) {
          try {
            instantiator = new JDBCObjectStoreEntryReaderInstantiator(
                DatabaseType.databaseType(connection), clonedConfiguration.connectionProvider, name);
            connection.commit();
          } catch (Exception e) {
            connection.rollback();

            String message = "Failed to get entryReader because: " + e.getMessage();
            logger.error(message, e);
            throw new IllegalArgumentException(message);
          }
        } catch (Exception e) {
          String message = "Failed (connection) to get entryReader because: " + e.getMessage();
          logger.error(message, e);
          throw new IllegalStateException(message);
        }

        break;
      default:
        throw new IllegalStateException(getClass().getSimpleName() + ": Cannot create entry reader '" + name + "' due to unknown type: " + delegate.type());
      }

      entryReader = stage().actorFor(ObjectStoreEntryReader.class, Definition.has(actorType, instantiator), address);
    }

    return completes().with(entryReader);
  }

  @Override
  public <T extends StateObject, E> void persist(StateSources<T, E> stateSources, Metadata metadata, long updateId, PersistResultInterest interest, Object object) {
    final List<Source<E>> sources = stateSources.sources();
    final T persistentObject = stateSources.stateObject();
    try {
      delegate.beginTransaction();

      final State<?> state = delegate.persist(persistentObject, updateId, metadata);

      final int entryVersion = (int) stateSources.stateObject().version();
      final List<Entry<?>> entries = entryAdapterProvider.asEntries(sources, entryVersion, metadata);
      delegate.persistEntries(entries);

      final Dispatchable<Entry<?>, State<?>> dispatchable = buildDispatchable(state, entries);
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
    final List<Dispatchable<Entry<?>, State<?>>> allDispatchables = new ArrayList<>();
    try {
      delegate.beginTransaction();
      for (StateSources<T,E> stateSources : allStateSources) {
        final T persistentObject = stateSources.stateObject();
        final List<Source<E>> sources = stateSources.sources();

        final int entryVersion = (int) stateSources.stateObject().version();
        final List<Entry<?>> entries = entryAdapterProvider.asEntries(sources, entryVersion, metadata);
        delegate.persistEntries(entries);

        final State<?> state = delegate.persist(persistentObject, updateId, metadata);
        allPersistentObjects.add(persistentObject);

        final Dispatchable<Entry<?>, State<?>> dispatchable = buildDispatchable(state, entries);
        delegate.persistDispatchable(dispatchable);
        allDispatchables.add(dispatchable);
      }
      delegate.completeTransaction();

      //Dispatch after commit
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
   * @see io.vlingo.xoom.symbio.store.object.ObjectStore#queryAll(io.vlingo.xoom.symbio.store.object.QueryExpression, io.vlingo.xoom.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
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
   * @see io.vlingo.xoom.symbio.store.object.ObjectStore#queryObject(io.vlingo.xoom.symbio.store.object.QueryExpression, io.vlingo.xoom.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
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
  public void intervalSignal(final Scheduled<Object> scheduled, final Object data) {
    delegate.timeoutCheck();
  }

  /*
   * @see io.vlingo.xoom.actors.Actor#stop()
   */
  @Override
  public void stop() {
    close();

    super.stop();
  }

  private Dispatchable<Entry<?>, State<?>> buildDispatchable(final State<?> state, final List<Entry<?>> entries){
    final String id = identityGenerator.generate().toString();
    return new Dispatchable<>(id, LocalDateTime.now(), state, entries);
  }
}
