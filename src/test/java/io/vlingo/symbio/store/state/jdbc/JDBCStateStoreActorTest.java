// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.actors.ActorInstantiator;
import io.vlingo.actors.Definition;
import io.vlingo.actors.World;
import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.reactivestreams.Stream;
import io.vlingo.reactivestreams.sink.ConsumerSink;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.State;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.StateBundle;
import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.ListQueryExpression;
import io.vlingo.symbio.store.QueryExpression;
import io.vlingo.symbio.store.Result;
import io.vlingo.symbio.store.common.event.TestEvent;
import io.vlingo.symbio.store.common.event.TestEventAdapter;
import io.vlingo.symbio.store.common.jdbc.Configuration.TestConfiguration;
import io.vlingo.symbio.store.state.Entity1;
import io.vlingo.symbio.store.state.Entity1.Entity1StateAdapter;
import io.vlingo.symbio.store.state.MockResultInterest;
import io.vlingo.symbio.store.state.MockResultInterest.StoreData;
import io.vlingo.symbio.store.state.MockTextDispatcher;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateStore.StorageDelegate;
import io.vlingo.symbio.store.state.StateStore.TypedStateBundle;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
import io.vlingo.symbio.store.state.jdbc.JDBCStateStoreActor.JDBCStateStoreInstantiator;

public abstract class JDBCStateStoreActorTest {
  protected TestConfiguration configuration;
  protected StorageDelegate delegate;
  protected MockTextDispatcher dispatcher;
  protected String entity1StoreName;
  protected MockResultInterest interest;
  protected StateStore store;
  protected World world;

  @Test
  public void testThatStateStoreReadsWrites() throws Exception {
    final AccessSafely accessInterest = interest.afterCompleting(6);
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(6);

    final String entity1Id = "123";
    final Entity1 entity1 = new Entity1(entity1Id, 1);
    store.write(entity1.id, entity1, 1, interest);
    final String entity2Id = "234";
    final Entity1 entity2 = new Entity1(entity2Id, 2);
    store.write(entity2.id, entity2, 1, interest);
    final String entity3Id = "345";
    final Entity1 entity3 = new Entity1(entity3Id, 3);
    store.write(entity3.id, entity3, 1, interest);

    assertEquals(3, (int) accessDispatcher.readFrom("dispatchedStateCount"));
    assertEquals(3, (int) accessInterest.readFrom("confirmDispatchedResultedIn"));

    final AccessSafely accessInterest1 = interest.afterCompleting(1);
    store.read(entity1Id, Entity1.class, interest);
    final Entity1 entity1_1 = accessInterest1.readFrom("readStoreData");
    assertEquals(entity1, entity1_1);

    final AccessSafely accessInterest2 = interest.afterCompleting(1);
    store.read(entity2Id, Entity1.class, interest);
    final Entity1 entity2_1 = accessInterest2.readFrom("readStoreData");
    assertEquals(entity2, entity2_1);

    final AccessSafely accessInterest3 = interest.afterCompleting(1);
    store.read(entity3Id, Entity1.class, interest);
    final Entity1 entity3_1 = accessInterest3.readFrom("readStoreData");
    assertEquals(entity3, entity3_1);
  }

  @Test
  public void testThatStateStoreDispatches() throws Exception {
    final AccessSafely accessInterest1 = interest.afterCompleting(6);
    final AccessSafely accessDispatcher1 = dispatcher.afterCompleting(6);

    final Entity1 entity1 = new Entity1("123", 1);
    store.write(entity1.id, entity1, 1, interest);
    final Entity1 entity2 = new Entity1("234", 2);
    store.write(entity2.id, entity2, 1, interest);
    final Entity1 entity3 = new Entity1("345", 3);
    store.write(entity3.id, entity3, 1, interest);

    assertEquals(3, (int) accessDispatcher1.readFrom("dispatchedStateCount"));
    assertEquals(3, (int) accessInterest1.readFrom("confirmDispatchedResultedIn"));
    final State<?> state123 = accessDispatcher1.readFrom("dispatchedState", dispatchId("123"));
    assertEquals("123", state123.id);
    final State<?> state234 = accessDispatcher1.readFrom("dispatchedState", dispatchId("234"));
    assertEquals("234", state234.id);
    final State<?> state345 = accessDispatcher1.readFrom("dispatchedState", dispatchId("345"));
    assertEquals("345", state345.id);

    final AccessSafely accessInterest2 = interest.afterCompleting(4);
    final AccessSafely accessDispatcher2 = dispatcher.afterCompleting(2);

    final Entity1 entity4 = new Entity1("456", 4);
    store.write(entity4.id, entity4, 1, interest);
    final Entity1 entity5 = new Entity1("567", 5);
    store.write(entity5.id, entity5, 1, interest);

    assertTrue(4 <= (int) accessDispatcher2.readFrom("dispatchedStateCount"));
    assertEquals(5, (int) accessInterest2.readFrom("confirmDispatchedResultedIn"));
    final State<?> state456 = accessDispatcher2.readFrom("dispatchedState", dispatchId("456"));
    assertEquals("456", state456.id);
    final State<?> state567 = accessDispatcher2.readFrom("dispatchedState", dispatchId("567"));
    assertEquals("567", state567.id);
  }

  @Test
  public void testThatReadAllReadsAll() {
    final AccessSafely accessWrites = interest.afterCompleting(6);

    final Entity1 entity1 = new Entity1("123", 1);
    store.write(entity1.id, entity1, 1, interest);
    final Entity1 entity2 = new Entity1("234", 2);
    store.write(entity2.id, entity2, 1, interest);
    final Entity1 entity3 = new Entity1("345", 3);
    store.write(entity3.id, entity3, 1, interest);

    final int totalWrites = accessWrites.readFrom("totalWrites");

    assertEquals(3, totalWrites);

    final AccessSafely accessReads = interest.afterCompleting(3);

    final List<TypedStateBundle> bundles =
            Arrays.asList(
                    new TypedStateBundle(entity1.id, Entity1.class),
                    new TypedStateBundle(entity2.id, Entity1.class),
                    new TypedStateBundle(entity3.id, Entity1.class));

    store.readAll(bundles, interest, null);

    final List<StoreData<?>> allStates = accessReads.readFrom("readAllStates");

    assertEquals(3, allStates.size());
    final Entity1 state123 = allStates.get(0).typedState();
    assertEquals("123", state123.id);
    assertEquals(1, state123.value);
    final Entity1 state234 = allStates.get(1).typedState();
    assertEquals("234", state234.id);
    assertEquals(2, state234.value);
    final Entity1 state345 = allStates.get(2).typedState();
    assertEquals("345", state345.id);
    assertEquals(3, state345.value);
  }

  @Test
  public void testThatReadErrorIsReported() {
    final AccessSafely accessInterest1 = interest.afterCompleting(2);
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(2);

    final Entity1 entity = new Entity1("123", 1);
    store.write(entity.id, entity, 1, interest);
    assertEquals(1, (int) accessDispatcher.readFrom("dispatchedStateCount"));
    assertEquals(1, (int) accessInterest1.readFrom("confirmDispatchedResultedIn"));

    final AccessSafely accessInterest2 = interest.afterCompleting(1);
    store.read(null, Entity1.class, interest);

    assertEquals(1, (int) accessInterest2.readFrom("errorCausesCount"));
    final Exception cause1 = accessInterest2.readFrom("errorCauses");
    assertEquals("The id is null.", cause1.getMessage());
    Result result1 = accessInterest2.readFrom("textReadResult");
    assertTrue(result1.isError());
    assertNull(accessInterest2.readFrom("stateHolder"));

    interest = new MockResultInterest();
    final AccessSafely accessInterest3 = interest.afterCompleting(1);
    dispatcher.afterCompleting(1);

    store.read(entity.id, null, interest);  // includes read

    assertEquals(1, (int) accessInterest3.readFrom("errorCausesCount"));
    final Exception cause2 = accessInterest3.readFrom("errorCauses");
    assertEquals("The type is null.", cause2.getMessage());
    Result result2 = accessInterest3.readFrom("textReadResult");
    assertTrue(result2.isError());
    final Object objectState = accessInterest3.readFrom("stateHolder");
    assertNull(objectState);
  }

  @Test
  public void testThatWriteErrorIsReported() {
    final AccessSafely accessInterest1 = interest.afterCompleting(1);
    dispatcher.afterCompleting(1);

    store.write(null, null, 0, interest);

    assertEquals(1, (int) accessInterest1.readFrom("errorCausesCount"));
    final Exception cause1 = accessInterest1.readFrom("errorCauses");
    assertEquals("The state is null.", cause1.getMessage());
    final Result result1 = accessInterest1.readFrom("textWriteAccumulatedResults");
    assertTrue(result1.isError());
    final Object objectState = accessInterest1.readFrom("stateHolder");
    assertNull(objectState);
  }

  @Test
  public void testRedispatch() {
    final AccessSafely accessInterest = interest.afterCompleting(6);
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(6);

    final Entity1 entity1 = new Entity1(UUID.randomUUID().toString(), 1);
    final TestEvent testEvent1 = new TestEvent(UUID.randomUUID().toString(), 30);
    store.write(entity1.id, entity1, 1, Collections.singletonList(testEvent1), interest);

    final Entity1 entity2 = new Entity1(UUID.randomUUID().toString(), 2);
    final TestEvent testEvent2 = new TestEvent(UUID.randomUUID().toString(), 45);
    store.write(entity2.id, entity2, 1, Collections.singletonList(testEvent2), interest);


    final Entity1 entity3 = new Entity1(UUID.randomUUID().toString(), 3);
    final TestEvent testEvent3 = new TestEvent(UUID.randomUUID().toString(), 12);
    store.write(entity3.id, entity3, 1, Arrays.asList(testEvent1, testEvent2, testEvent3), interest);

    final int confirmDispatchedResultedIn = accessInterest.readFrom("confirmDispatchedResultedIn");
    assertEquals("confirmDispatchedResultedIn", 3, confirmDispatchedResultedIn);

    final int writeTextResultedIn = accessInterest.readFrom("writeTextResultedIn");
    assertEquals("writeTextResultedIn", 3, writeTextResultedIn);

    final int dispatchedStateCount = accessDispatcher.readFrom("dispatchedStateCount");
    assertEquals("dispatchedStateCount", 3, dispatchedStateCount);

    final int dispatchAttemptCount = accessDispatcher.readFrom("dispatchAttemptCount");
    assertEquals("dispatchAttemptCount", 3, dispatchAttemptCount);
  }

  private ConsumerSink<StateBundle> sink;

  private AtomicInteger totalStates = new AtomicInteger(0);

  @Test
  public void testThatAllOfTypeStreams() {
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(2 * 200);
    for (int count = 1; count <= 200; ++count) {
      final Entity1 entity1 = new Entity1("" + count, count);
      store.write(entity1.id, entity1, 1, interest);
    }

    assertEquals(200, (int) accessDispatcher.readFrom("dispatchedStateCount"));

    final Stream all = store.streamAllOf(Entity1.class).await();

    final AccessSafely access = AccessSafely.afterCompleting(200);

    access.writingWith("stateCounter", (state) -> { totalStates.incrementAndGet(); });
    access.readingWith("stateCount", () -> totalStates.get());

    all.flowInto(new ConsumerSink<>((state) -> access.writeUsing("stateCounter", 1)), 10);

    final int stateCount = access.readFromExpecting("stateCount", 200);

    Assert.assertEquals(totalStates.get(), 200);
    Assert.assertEquals(totalStates.get(), stateCount);
  }

  @Test
  public void testThatAllOfTypeStreamsUntilStop() {
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(2 * 100);
    for (int count = 1; count <= 100; ++count) {
      final Entity1 entity1 = new Entity1("" + count, count);
      store.write(entity1.id, entity1, 1, interest);
    }

    assertEquals(100, (int) accessDispatcher.readFrom("dispatchedStateCount"));

    final Stream all = store.streamAllOf(Entity1.class).await();

    final AccessSafely access = AccessSafely.afterCompleting(20);

    access.writingWith("stateCounter", (state) -> { totalStates.incrementAndGet(); });
    access.readingWith("stateCount", () -> totalStates.get());

    final Consumer<StateBundle> bundles =
            (state) -> {
              access.writeUsing("stateCounter", 1);
              final int count = totalStates.get();
              if (count == 20) {
                sink.terminate();
                all.stop();
                System.out.println("STOPPED");
              }
            };

    sink = new ConsumerSink<>(bundles);

    all.flowInto(sink, 50);

    final int stateCount = access.readFrom("stateCount");

    Assert.assertNotEquals(100, stateCount);
    Assert.assertEquals(20, totalStates.get());
  }

  @Test
  public void testThatAllOfTypeStreamsAdjusting() {
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(2 * 100);
    for (int count = 1; count <= 100; ++count) {
      final Entity1 entity1 = new Entity1("" + count, count);
      store.write(entity1.id, entity1, count, interest);
    }

    assertEquals(100, (int) accessDispatcher.readFrom("dispatchedStateCount"));

    final Stream all = store.streamAllOf(Entity1.class).await();

    final AccessSafely access = AccessSafely.afterCompleting(100);

    access.writingWith("stateCounter", (state) -> { totalStates.incrementAndGet(); });
    access.readingWith("stateCount", () -> totalStates.get());

    all.flowInto(new ConsumerSink<>((StateBundle state) -> {
            access.writeUsing("stateCounter", 1);
            final int count = totalStates.get();
            if (count >= 10) {
              all.request(20);
            }
          }));

    final int stateCount = access.readFrom("stateCount");

    Assert.assertEquals(100, stateCount);
    Assert.assertEquals(100, totalStates.get());
  }

  @Test
  public void testThatSomeOfTypeStreamsAllFound() {
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(2 * 50);
    for (int count = 1; count <= 50; ++count) {
      final Entity1 entity1 = new Entity1("" + count, count);
      store.write(entity1.id, entity1, 1, interest);
    }

    assertEquals(50, (int) accessDispatcher.readFrom("dispatchedStateCount"));

    final QueryExpression query = QueryExpression.using(Entity1.class, someOfTypeStreams(Entity1.class));

    final Stream all = store.streamSomeUsing(query).await();

    final AccessSafely access = AccessSafely.afterCompleting(5);

    access.writingWith("stateCounter", (state) -> { totalStates.incrementAndGet(); });
    access.readingWith("stateCount", () -> totalStates.get());

    all.flowInto(new ConsumerSink<>((state) -> {
      access.writeUsing("stateCounter", 1);
    }));

    final int stateCount = access.readFrom("stateCount");

    Assert.assertNotEquals(100, stateCount);
    Assert.assertEquals(5, totalStates.get());
  }

  @Test
  public void testThatSomeOfTypeStreamsFromList() {
    final AccessSafely accessDispatcher = dispatcher.afterCompleting(2 * 50);
    for (int count = 1; count <= 50; ++count) {
      final Entity1 entity1 = new Entity1("" + count, count);
      store.write(entity1.id, entity1, 1, interest);
    }

    assertEquals(50, (int) accessDispatcher.readFrom("dispatchedStateCount"));

    final ListQueryExpression query =
            ListQueryExpression.using(
                    Entity1.class,
                    someOfTypeStreamsWithParameters(Entity1.class),
                    Arrays.asList(21, 25));

    final Stream all = store.streamSomeUsing(query).await();

    final AccessSafely access = AccessSafely.afterCompleting(5);

    access.writingWith("stateCounter", (state) -> { totalStates.incrementAndGet(); });
    access.readingWith("stateCount", () -> totalStates.get());

    all.flowInto(new ConsumerSink<>((state) -> {
      access.writeUsing("stateCounter", 1);
    }));

    final int stateCount = access.readFromExpecting("stateCount", 5);

    Assert.assertNotEquals(100, stateCount);
    Assert.assertEquals(5, totalStates.get());
  }

  @Before
  public void setUp() throws Exception {
    world = World.startWithDefaults("test-store");

    entity1StoreName = Entity1.class.getSimpleName();
    StateTypeStateStoreMap.stateTypeToStoreName(Entity1.class, entity1StoreName);

    configuration = testConfiguration(DataFormat.Text);

    delegate = delegate();

    interest = new MockResultInterest();
    interest.afterCompleting(0); // avoid NPE
    dispatcher = new MockTextDispatcher(0, interest);
    dispatcher.afterCompleting(0); // avoid NPE

    EntryAdapterProvider.instance(world).registerAdapter(TestEvent.class, new TestEventAdapter());
    StateAdapterProvider.instance(world).registerAdapter(Entity1.class, new Entity1StateAdapter());
    // NOTE: No adapter registered for Entity2.class because it will use the default

    JDBCEntriesWriter entriesWriter = world.stage().actorFor(JDBCEntriesWriter.class, JDBCEntriesWriterActor.class, delegate.copy(), Arrays.asList(dispatcher));
    final ActorInstantiator<?> instantiator = new JDBCStateStoreInstantiator();
    instantiator.set("delegate", delegate);
    instantiator.set("entriesWriter", entriesWriter);

    store = world.actorFor(
            StateStore.class,
            Definition.has(JDBCStateStoreActor.class, instantiator));
  }

  @After
  public void tearDown() throws Exception {
    if (configuration == null) return;
    world.terminate();
    configuration.cleanUp();
    delegate.close();
  }

  protected abstract StorageDelegate delegate() throws Exception;
  protected abstract TestConfiguration testConfiguration(final DataFormat format) throws Exception;
  protected abstract String someOfTypeStreams(final Class<?> type);
  protected abstract String someOfTypeStreamsWithParameters(final Class<?> type);

  protected String tableName(final Class<?> type) {
    return ("tbl_"+StateTypeStateStoreMap.storeNameFrom(type)).toLowerCase();
  }

  private String dispatchId(final String entityId) {
    return entity1StoreName + ":" + entityId;
  }
}
