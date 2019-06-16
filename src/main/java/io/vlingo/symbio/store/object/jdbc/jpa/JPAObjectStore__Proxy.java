package io.vlingo.symbio.store.object.jdbc.jpa;

import java.util.Collection;
import java.util.List;

import io.vlingo.actors.Actor;
import io.vlingo.actors.DeadLetter;
import io.vlingo.actors.LocalMessage;
import io.vlingo.actors.Mailbox;
import io.vlingo.symbio.Source;
import io.vlingo.symbio.store.object.PersistentObject;

public class JPAObjectStore__Proxy implements io.vlingo.symbio.store.object.jdbc.jpa.JPAObjectStore {

  private static final String removeRepresentation1 = "remove(T, long, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest)";
  private static final String removeRepresentation2 = "remove(T, long, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest, java.lang.Object)";
  private static final String closeRepresentation3 = "close()";
  private static final String registerMapperRepresentation4 = "registerMapper(io.vlingo.symbio.store.object.PersistentObjectMapper)";
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
  public <T extends io.vlingo.symbio.store.object.PersistentObject>void remove(T arg0, long arg1, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.remove(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, removeRepresentation1); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, removeRepresentation1)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, removeRepresentation1));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.PersistentObject>void remove(T arg0, long arg1, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg2, java.lang.Object arg3) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.remove(arg0, arg1, arg2, arg3);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, removeRepresentation2); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, removeRepresentation2)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, removeRepresentation2));
    }
  }
  @Override
  public void close() {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.close();
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, closeRepresentation3); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, closeRepresentation3)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, closeRepresentation3));
    }
  }
  @Override
  public void registerMapper(io.vlingo.symbio.store.object.PersistentObjectMapper arg0) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.registerMapper(arg0);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, registerMapperRepresentation4); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, registerMapperRepresentation4)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, registerMapperRepresentation4));
    }
  }
  @Override
  public void queryObject(io.vlingo.symbio.store.object.QueryExpression arg0, io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest arg1, java.lang.Object arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.queryObject(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, queryObjectRepresentation5); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, queryObjectRepresentation5)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, queryObjectRepresentation5));
    }
  }
  @Override
  public void queryObject(io.vlingo.symbio.store.object.QueryExpression arg0, io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest arg1) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.queryObject(arg0, arg1);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, queryObjectRepresentation6); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, queryObjectRepresentation6)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, queryObjectRepresentation6));
    }
  }
  @Override
  public void queryAll(io.vlingo.symbio.store.object.QueryExpression arg0, io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest arg1) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.queryAll(arg0, arg1);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, queryAllRepresentation7); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, queryAllRepresentation7)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, queryAllRepresentation7));
    }
  }
  @Override
  public void queryAll(io.vlingo.symbio.store.object.QueryExpression arg0, io.vlingo.symbio.store.object.ObjectStoreReader.QueryResultInterest arg1, java.lang.Object arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.queryAll(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, queryAllRepresentation8); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, queryAllRepresentation8)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, queryAllRepresentation8));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.PersistentObject>void persist(T arg0, long arg1, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persist(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistRepresentation9); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, persistRepresentation9)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistRepresentation9));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.PersistentObject>void persist(T arg0, long arg1, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg2, java.lang.Object arg3) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persist(arg0, arg1, arg2, arg3);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistRepresentation11); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, persistRepresentation11)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistRepresentation11));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.PersistentObject>void persist(T arg0, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg1) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persist(arg0, arg1);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistRepresentation13); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, persistRepresentation13)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistRepresentation13));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.PersistentObject>void persist(T arg0, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg1, java.lang.Object arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persist(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistRepresentation15); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, persistRepresentation15)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistRepresentation15));
    }
  }
  @Override
  public <T extends PersistentObject, E> void persist(T arg0, List<Source<E>> arg1, long arg2, PersistResultInterest arg3, Object arg4) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persist(arg0, arg1, arg2, arg3, arg4);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistRepresentation15); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, persistRepresentation15)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistRepresentation15));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.PersistentObject>void persistAll(java.util.Collection<T> arg0, long arg1, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persistAll(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistAllRepresentation18); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, persistAllRepresentation18)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistAllRepresentation18));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.PersistentObject>void persistAll(java.util.Collection<T> arg0, long arg1, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg2, java.lang.Object arg3) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persistAll(arg0, arg1, arg2, arg3);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistAllRepresentation19); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, persistAllRepresentation19)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistAllRepresentation19));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.PersistentObject>void persistAll(java.util.Collection<T> arg0, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg1) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persistAll(arg0, arg1);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistAllRepresentation21); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, persistAllRepresentation21)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistAllRepresentation21));
    }
  }
  @Override
  public <T extends io.vlingo.symbio.store.object.PersistentObject>void persistAll(java.util.Collection<T> arg0, io.vlingo.symbio.store.object.ObjectStoreWriter.PersistResultInterest arg1, java.lang.Object arg2) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persistAll(arg0, arg1, arg2);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistAllRepresentation23); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, persistAllRepresentation23)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistAllRepresentation23));
    }
  }
  @Override
  public <T extends PersistentObject, E> void persistAll(Collection<T> arg0, List<Source<E>> arg1, long arg2, PersistResultInterest arg3, Object arg4) {
    if (!actor.isStopped()) {
      final java.util.function.Consumer<JPAObjectStore> consumer = (actor) -> actor.persistAll(arg0, arg1, arg2, arg3, arg4);
      if (mailbox.isPreallocated()) { mailbox.send(actor, JPAObjectStore.class, consumer, null, persistAllRepresentation23); }
      else { mailbox.send(new LocalMessage<JPAObjectStore>(actor, JPAObjectStore.class, consumer, persistAllRepresentation23)); }
    } else {
      actor.deadLetters().failedDelivery(new DeadLetter(actor, persistAllRepresentation23));
    }
  }
}
