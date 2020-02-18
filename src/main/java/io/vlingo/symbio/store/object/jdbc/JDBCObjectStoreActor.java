// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vlingo.actors.Actor;
import io.vlingo.actors.ActorInstantiator;
import io.vlingo.actors.Address;
import io.vlingo.actors.Definition;
import io.vlingo.actors.Logger;
import io.vlingo.common.Completes;
import io.vlingo.common.Failure;
import io.vlingo.common.Scheduled;
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
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.dispatch.DispatcherControl.DispatcherControlInstantiator;
import io.vlingo.symbio.store.dispatch.control.DispatcherControlActor;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.ObjectStoreEntryReader;
import io.vlingo.symbio.store.object.QueryExpression;
import io.vlingo.symbio.store.object.StateObject;
import io.vlingo.symbio.store.object.StateSources;
import io.vlingo.symbio.store.object.jdbc.JDBCObjectStoreEntryReaderActor.JDBCObjectStoreEntryReaderInstantiator;
import io.vlingo.symbio.store.object.jdbc.jdbi.JdbiObjectStoreEntryReaderActor;
import io.vlingo.symbio.store.object.jdbc.jdbi.JdbiObjectStoreEntryReaderActor.JdbiObjectStoreEntryReaderInstantiator;
import io.vlingo.symbio.store.object.jdbc.jdbi.JdbiOnDatabase;

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

//  public JDBCObjectStoreActor(final JDBCObjectStoreDelegate delegate, final Dispatcher<Dispatchable<Entry<?>, State<?>>> dispatcher) {
//     this(delegate, dispatcher, DefaultCheckConfirmationExpirationInterval, DefaultConfirmationExpiration);
//  }

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

//  public JDBCObjectStoreActor(
//          final JDBCObjectStoreDelegate delegate,
//          final Dispatcher<Dispatchable<Entry<?>, State<?>>> dispatcher,
//          final long checkConfirmationExpirationInterval,
//          final long confirmationExpiration) {
//    this(delegate, Arrays.asList(dispatcher), checkConfirmationExpirationInterval, confirmationExpiration);
//  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#close()
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
      final Configuration clonedConfiguration = Configuration.cloneOf(delegate.configuration);
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
        instantiator = new JDBCObjectStoreEntryReaderInstantiator(DatabaseType.databaseType(clonedConfiguration.connection), clonedConfiguration.connection, name);
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
   * @see io.vlingo.symbio.store.object.ObjectStore#queryAll(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
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
   * @see io.vlingo.symbio.store.object.ObjectStore#queryObject(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
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
   * @see io.vlingo.actors.Actor#stop()
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
