// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.symbio.store.object.jdbc.jpa;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Definition;
import io.vlingo.actors.Logger;
import io.vlingo.common.Failure;
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
import io.vlingo.symbio.store.object.PersistentObject;
import io.vlingo.symbio.store.object.PersistentObjectMapper;
import io.vlingo.symbio.store.object.QueryExpression;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * JPAObjectStoreActor
 */
public class JPAObjectStoreActor extends Actor implements JPAObjectStore {
  private final DispatcherControl dispatcherControl;
  private final Dispatcher<Dispatchable<Entry<String>, State<String>>> dispatcher;
  private boolean closed;
  private final JPAObjectStoreDelegate delegate;
  private final EntryAdapterProvider entryAdapterProvider;
  private final Logger logger;
  private final IdentityGenerator identityGenerator;
  private final StateAdapterProvider stateAdapterProvider;

  public JPAObjectStoreActor(final JPAObjectStoreDelegate delegate,
          final Dispatcher<Dispatchable<Entry<String>, State<String>>> dispatcher) {
    this(delegate, dispatcher, 1000L, 1000L);
  }

  public JPAObjectStoreActor(final JPAObjectStoreDelegate delegate,
          final Dispatcher<Dispatchable<Entry<String>, State<String>>> dispatcher,
          final long checkConfirmationExpirationInterval, final long confirmationExpiration) {
    this.delegate = delegate;
    this.dispatcher = dispatcher;
    this.closed = false;
    this.entryAdapterProvider = EntryAdapterProvider.instance(stage().world());
    this.stateAdapterProvider = StateAdapterProvider.instance(stage().world());
    this.logger = stage().world().defaultLogger();
    this.identityGenerator = new IdentityGenerator.TimeBasedIdentityGenerator();

    this.dispatcherControl = stage().actorFor(
            DispatcherControl.class,
            Definition.has(
                    DispatcherControlActor.class,
                    Definition.parameters(
                            dispatcher,
                            delegate,
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
  public <T extends PersistentObject, E> void persist(final T persistentObject, final List<Source<E>> sources, final Metadata metadata, final long updateId,
          final PersistResultInterest interest, final Object object) {

    try {
      delegate.beginWrite();
      final State.TextState raw = stateAdapterProvider.asRaw(String.valueOf(persistentObject.persistenceId()), persistentObject, 1, metadata);
      final List<Entry<String>> entries = entryAdapterProvider.asEntries(sources, metadata);

      final Dispatchable<Entry<String>, State<String>> dispatchable = buildDispatchable(raw, entries);

      delegate.persist(persistentObject, updateId, entries, dispatchable);

      delegate.complete();

      dispatcher.dispatch(dispatchable);

      interest.persistResultedIn(Success.of(Result.Success), persistentObject, 1, 1, object);
    } catch (final Exception e) {
      logger.error("Persist of: " + persistentObject + " failed because: " + e.getMessage(), e);
      delegate.fail();

      interest.persistResultedIn(Failure.of(new StorageException(Result.Failure, e.getMessage(), e)), persistentObject,
              1, 0, object);
    }
  }

  @Override
  public <T extends PersistentObject, E> void persistAll(final Collection<T> persistentObjects, final List<Source<E>> sources, final Metadata metadata,
          final long updateId, final PersistResultInterest interest, final Object object) {
    try {
      delegate.beginWrite();

      final List<Entry<String>> entries = entryAdapterProvider.asEntries(sources, metadata);

      final ArrayList<Dispatchable<Entry<String>, State<String>>> dispatchables = new ArrayList<>(persistentObjects.size());
      for (final T persistentObject : persistentObjects) {
        final State.TextState raw = stateAdapterProvider.asRaw(String.valueOf(persistentObject.persistenceId()), persistentObject, 1, metadata);
        dispatchables.add(buildDispatchable(raw, entries));
      }

      delegate.persistAll(persistentObjects, updateId, entries, dispatchables);

      delegate.complete();

      dispatchables.forEach(dispatcher::dispatch);

      interest.persistResultedIn(Success.of(Result.Success), persistentObjects, persistentObjects.size(), persistentObjects.size(), object);
    } catch (final Exception e) {
      logger.error("Persist all of: " + persistentObjects + " failed because: " + e.getMessage(), e);
      delegate.fail();

      interest.persistResultedIn(
              Failure.of(new StorageException(Result.Failure, e.getMessage(), e)),
              persistentObjects, persistentObjects.size(), 0,
              object);
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
    delegate.queryAll(expression, interest, object);
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
    delegate.queryObject(expression, interest, object);
  }

  /*
   * @see
   * io.vlingo.symbio.store.object.ObjectStore#registerMapper(io.vlingo.symbio.
   * store.object.PersistentObjectMapper)
   */
  @Override
  public void registerMapper(final PersistentObjectMapper mapper) {
    delegate.registerMapper(mapper);
  }

  @Override
  public <T extends PersistentObject> void remove(final T persistentObject, final long removeId, final PersistResultInterest interest, final Object object) {
    delegate.remove(persistentObject, removeId, interest);
  }

  private Dispatchable<Entry<String>, State<String>> buildDispatchable(final State<String> state, final List<Entry<String>> entries){
    final String id = identityGenerator.generate().toString();
    return new Dispatchable<>(id, LocalDateTime.now(), state, entries);
  }
}
