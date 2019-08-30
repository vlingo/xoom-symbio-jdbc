package io.vlingo.symbio.store.object.jdbc.jpa;

import java.util.Collection;
import java.util.List;

import io.vlingo.actors.Actor;
import io.vlingo.actors.DeadLetter;
import io.vlingo.actors.LocalMessage;
import io.vlingo.actors.Mailbox;
import io.vlingo.actors.Returns;
import io.vlingo.common.BasicCompletes;
import io.vlingo.common.Completes;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.store.EntryReader;
import io.vlingo.symbio.store.object.ObjectStore;
import io.vlingo.symbio.store.object.StateObject;

public class JPAObjectStore__Proxy implements io.vlingo.symbio.store.object.jdbc.jpa.JPAObjectStore {

  private static final String removeRepresentation1 = "remove(T, long, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest)";
  private static final String removeRepresentation2 = "remove(T, long, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest, java.lang.Object)";
  private static final String closeRepresentation3 = "close()";
  private static final String entryReaderRepresentation4 = "entryReader(java.lang.String)";
  private static final String queryObjectRepresentation5 = "queryObject(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest, java.lang.Object)";
  private static final String queryObjectRepresentation6 = "queryObject(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest)";
  private static final String queryAllRepresentation7 = "queryAll(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest)";
  private static final String queryAllRepresentation8 = "queryAll(io.vlingo.symbio.store.object.QueryExpression, io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest, java.lang.Object)";
  private static final String persistRepresentation9 = "persist(T, long, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest)";
  private static final String persistRepresentation11 = "persist(T, long, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest, java.lang.Object)";
  private static final String persistRepresentation13 = "persist(T, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest)";
  private static final String persistRepresentation15 = "persist(T, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest, java.lang.Object)";
  private static final String persistAllRepresentation18 = "persistAll(java.util.Collection<T>, long, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest)";
  private static final String persistAllRepresentation19 = "persistAll(java.util.Collection<T>, long, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest, java.lang.Object)";
  private static final String persistAllRepresentation21 = "persistAll(java.util.Collection<T>, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest)";
  private static final String persistAllRepresentation23 = "persistAll(java.util.Collection<T>, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest, java.lang.Object)";

  private final Actor actor;
  private final Mailbox mailbox;

  public JPAObjectStore__Proxy(final Actor actor, final Mailbox mailbox){
    this.actor = actor;
    this.mailbox = mailbox;
  }

  @Override
  public <T extends io.vlingo.symbio.store.object.StateObject>void remove(final T arg0, final long arg1, final io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.remove(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, removeRepresentation1); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, removeRepresentation1)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, removeRepresentation1));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.StateObject>void remove(final T arg0, final long arg1, final io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg2, final java.lang.Object arg3) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.remove(arg0, arg1, arg2, arg3);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, removeRepresentation2); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, removeRepresentation2)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, removeRepresentation2));
    }
  }
  @Override
  public void close() {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = ObjectStore::close;
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, closeRepresentation3); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, closeRepresentation3)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, closeRepresentation3));
    }
  }
  @Override
  public Completes<EntryReader<? extends Entry<?>>> entryReader(final String arg0) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<ObjectStore> consumer = (actor) -> actor.entryReader(arg0);
      final Completes<EntryReader<? extends Entry<?>>> completes = new BasicCompletes<>(actor.scheduler());
      if (mailbox.isPreallocated()) { mailbox.send(actor, ObjectStore.class, consumer, completes, entryReaderRepresentation4); }
      else { mailbox.send(new LocalMessage<ObjectStore>(actor, ObjectStore.class, consumer, Returns.value(completes), entryReaderRepresentation4)); }
      return completes;
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, entryReaderRepresentation4));
    }
    return null;
  }
  @Override
  public void queryObject(final io.vlingo.symbio.store.object.QueryExpression arg0, final io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest arg1, final java.lang.Object arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.queryObject(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, queryObjectRepresentation5); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, queryObjectRepresentation5)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, queryObjectRepresentation5));
    }
  }
  @Override
  public void queryObject(final io.vlingo.symbio.store.object.QueryExpression arg0, final io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest arg1) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.queryObject(arg0, arg1);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, queryObjectRepresentation6); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, queryObjectRepresentation6)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, queryObjectRepresentation6));
    }
  }
  @Override
  public void queryAll(final io.vlingo.symbio.store.object.QueryExpression arg0, final io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest arg1) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.queryAll(arg0, arg1);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, queryAllRepresentation7); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, queryAllRepresentation7)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, queryAllRepresentation7));
    }
  }
  @Override
  public void queryAll(final io.vlingo.symbio.store.object.QueryExpression arg0, final io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest arg1, final java.lang.Object arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.queryAll(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, queryAllRepresentation8); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, queryAllRepresentation8)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, queryAllRepresentation8));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.StateObject>void persist(final T arg0, final long arg1, final io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persist(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistRepresentation9); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, persistRepresentation9)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistRepresentation9));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.StateObject>void persist(final T arg0, final long arg1, final io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg2, final java.lang.Object arg3) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persist(arg0, arg1, arg2, arg3);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistRepresentation11); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, persistRepresentation11)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistRepresentation11));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.StateObject>void persist(final T arg0, final io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg1) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persist(arg0, arg1);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistRepresentation13); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, persistRepresentation13)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistRepresentation13));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.StateObject>void persist(final T arg0, final io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg1, final java.lang.Object arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persist(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistRepresentation15); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, persistRepresentation15)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistRepresentation15));
    }
  }
  @Override
  public <T extends StateObject, E> void persist(final T arg0, final List<Source<E>> arg1, final long arg2, final PersistResultInterest arg3, final Object arg4) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persist(arg0, arg1, arg2, arg3, arg4);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistRepresentation15); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, persistRepresentation15)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistRepresentation15));
    }
  }

  @Override
  public <T extends StateObject, E> void persist(
          final T arg0, final List<Source<E>> arg1, final Metadata arg2, final long arg3, final PersistResultInterest arg4, final Object arg5) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persist(arg0, arg1, arg2, arg3, arg4, arg5);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistRepresentation15); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, persistRepresentation15)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistRepresentation15));
    }
  }

  @Override
  public <T extends io.vlingo.symbio.store.object.StateObject>void persistAll(final java.util.Collection<T> arg0, final long arg1, final io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persistAll(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistAllRepresentation18); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, persistAllRepresentation18)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistAllRepresentation18));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.StateObject>void persistAll(final java.util.Collection<T> arg0, final long arg1, final io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg2, final java.lang.Object arg3) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persistAll(arg0, arg1, arg2, arg3);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistAllRepresentation19); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, persistAllRepresentation19)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistAllRepresentation19));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.StateObject>void persistAll(final java.util.Collection<T> arg0, final io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg1) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persistAll(arg0, arg1);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistAllRepresentation21); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, persistAllRepresentation21)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistAllRepresentation21));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.StateObject>void persistAll(final java.util.Collection<T> arg0, final io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg1, final java.lang.Object arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persistAll(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistAllRepresentation23); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, persistAllRepresentation23)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistAllRepresentation23));
    }
  }
  @Override
  public <T extends StateObject, E> void persistAll(final Collection<T> arg0, final List<Source<E>> arg1, final long arg2, final PersistResultInterest arg3, final Object arg4) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persistAll(arg0, arg1, arg2, arg3, arg4);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistAllRepresentation23); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, persistAllRepresentation23)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistAllRepresentation23));
    }
  }

  @Override
  public <T extends StateObject, E> void persistAll(
          final Collection<T> arg0, final List<Source<E>> arg1, final Metadata arg2, final long arg3, final PersistResultInterest arg4, final Object arg5) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persistAll(arg0, arg1, arg2, arg3, arg4, arg5);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistAllRepresentation23); }
      else { mailbox.send(new LocalMessage<>(actor, JPAObjectStore.class, consumer, persistAllRepresentation23)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistAllRepresentation23));
    }
  }
}
