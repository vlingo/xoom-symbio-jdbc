// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Definition;
import io.vlingo.actors.Logger;
import io.vlingo.common.Failure;
import io.vlingo.common.Scheduled;
import io.vlingo.common.Success;
import io.vlingo.common.identity.IdentityGenerator;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.StorageException;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.Dispatcher;
import io.vlingo.symbio.store.dispatch.DispatcherControl;
import io.vlingo.symbio.store.dispatch.control.DispatcherControlActor;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The actor implementing the {@code ObjectStore} protocol in behalf of
 * any number of {@code JDBCObjectStoreDelegate} types.
 */
public class JDBCObjectStoreActor extends Actor implements ObjectStore, Scheduled<Object> {
  private final DispatcherControl dispatcherControl;
  private boolean closed;
  private final JDBCObjectStoreDelegate delegate;
  private final JDBCObjectStoreDelegate dispatchControlDelegate;
  private final Dispatcher<Dispatchable<Entry<?>, State<?>>> dispatcher;
  private final Logger logger;
  private final EntryAdapterProvider entryAdapterProvider;
  private final StateAdapterProvider stateAdapterProvider;
  private final IdentityGenerator identityGenerator;

  public JDBCObjectStoreActor(final JDBCObjectStoreDelegate delegate, final Dispatcher<Dispatchable<Entry<?>, State<?>>> dispatcher) {
     this(delegate, dispatcher, 1000L, 1000L);
  }

  @SuppressWarnings("unchecked")
  public JDBCObjectStoreActor(final JDBCObjectStoreDelegate delegate, final Dispatcher<Dispatchable<Entry<?>, State<?>>> dispatcher,
          final long checkConfirmationExpirationInterval, final long confirmationExpiration) {
    this.delegate = delegate;
    this.dispatcher = dispatcher;
    this.closed = false;
    this.logger = stage().world().defaultLogger();
    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());
    this.identityGenerator = new IdentityGenerator.RandomIdentityGenerator();

    final long timeout = delegate.configuration.transactionTimeoutMillis;
    stage().scheduler().schedule(selfAs(Scheduled.class), null, timeout, timeout);

    this.dispatchControlDelegate = delegate.copy();
    this.dispatcherControl = stage().actorFor(
            DispatcherControl.class,
            Definition.has(
                    DispatcherControlActor.class,
                    Definition.parameters(
                            dispatcher, dispatchControlDelegate,
                            checkConfirmationExpirationInterval,
                            confirmationExpiration)));
  }

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
  public <T extends PersistentObject, E> void persist(final T persistentObject, final List<Source<E>> sources, final Metadata metadata, final long updateId,
          final PersistResultInterest interest, final Object object) {
    try {
      delegate.beginWrite();

      final List<Entry<?>> entries = entryAdapterProvider.asEntries(sources, metadata);
      final State<?> raw = stateAdapterProvider.asRaw(String.valueOf(persistentObject.persistenceId()), persistentObject, 1, metadata);
      final Dispatchable<Entry<?>, State<?>> dispatchable = buildDispatchable(raw, entries);

      delegate.persist(persistentObject, updateId, entries, dispatchable);
      delegate.complete();

      dispatcher.dispatch(dispatchable);

      interest.persistResultedIn(Success.of(Result.Success), persistentObject, 1, 1, object);
    } catch (final Exception e) {
      delegate.fail();
      logger.error("Persist of: " + persistentObject + " failed because: " + e.getMessage(), e);

      interest.persistResultedIn(
              Failure.of(new StorageException(Result.Failure, e.getMessage(), e)),
              persistentObject, 1, 0,
              object);
    }
  }

  @Override
  public <T extends PersistentObject, E> void persistAll(final Collection<T> persistentObjects, final List<Source<E>> sources, final Metadata metadata,
          final long updateId, final PersistResultInterest interest, final Object object) {
    try {
      delegate.beginWrite();

      final List<Entry<?>> entries = entryAdapterProvider.asEntries(sources, metadata);
      final ArrayList<Dispatchable<Entry<?>, State<?>>> dispatchables = new ArrayList<>(persistentObjects.size());
      for (final T persistentObject : persistentObjects) {
        final State<?> raw = stateAdapterProvider.asRaw(String.valueOf(persistentObject.persistenceId()), persistentObject, 1, metadata);
        dispatchables.add(buildDispatchable(raw, entries));
      }

      delegate.persistAll(persistentObjects, updateId, entries, dispatchables);

      delegate.complete();

      dispatchables.forEach(dispatcher::dispatch);

      interest.persistResultedIn(Success.of(Result.Success), persistentObjects, persistentObjects.size(), persistentObjects.size(), object);
    } catch (final Exception e) {
      delegate.fail();
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
  public void queryAll(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    delegate.queryAll(expression, interest, object);
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#queryObject(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStore.QueryResultInterest, java.lang.Object)
   */
  @Override
  public void queryObject(final QueryExpression expression, final QueryResultInterest interest, final Object object) {
    delegate.queryObject(expression, interest, object);
  }

  /*
   * @see io.vlingo.symbio.store.object.ObjectStore#registerMapper(java.lang.Object)
   */
  @Override
  public void registerMapper(final PersistentObjectMapper mapper) {
    this.delegate.registerMapper(mapper);
    this.dispatchControlDelegate.registerMapper(mapper);
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
