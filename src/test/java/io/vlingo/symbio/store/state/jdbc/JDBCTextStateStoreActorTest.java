// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.state.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.actors.Definition;
import io.vlingo.actors.World;
import io.vlingo.actors.testkit.TestUntil;
import io.vlingo.symbio.store.state.Entity1;
import io.vlingo.symbio.store.state.Entity1.Entity1StateAdapter;
import io.vlingo.symbio.store.state.MockResultInterest;
import io.vlingo.symbio.store.state.MockTextDispatcher;
import io.vlingo.symbio.store.state.StateStore;
import io.vlingo.symbio.store.state.StateStore.DataFormat;
import io.vlingo.symbio.store.state.StateStore.StorageDelegate;
import io.vlingo.symbio.store.state.StateTypeStateStoreMap;
import io.vlingo.symbio.store.state.jdbc.Configuration.TestConfiguration;

public abstract class JDBCTextStateStoreActorTest {
  protected TestConfiguration configuration;
  protected StorageDelegate delegate;
  protected MockTextDispatcher dispatcher;
  protected String entity1StoreName;
  protected MockResultInterest interest;
  protected StateStore store;
  protected World world;

  @Test
  public void testThatStateStoreDispatches() throws Exception {
    interest.until = TestUntil.happenings(6);
    dispatcher.until = TestUntil.happenings(3);

    final Entity1 entity1 = new Entity1("123", 1);
    store.write(entity1.id, entity1, 1, interest);
    final Entity1 entity2 = new Entity1("234", 2);
    store.write(entity2.id, entity2, 1, interest);
    final Entity1 entity3 = new Entity1("345", 3);
    store.write(entity3.id, entity3, 1, interest);

    dispatcher.until.completes();
    interest.until.completes();

    assertEquals(3, dispatcher.dispatched.size());
    assertEquals(3, interest.confirmDispatchedResultedIn.get());
    assertEquals("123", dispatcher.dispatched.get(dispatchId("123")).id);
    assertEquals("234", dispatcher.dispatched.get(dispatchId("234")).id);
    assertEquals("345", dispatcher.dispatched.get(dispatchId("345")).id);

    interest.until = TestUntil.happenings(6);
    dispatcher.until = TestUntil.happenings(2);
    
    dispatcher.processDispatch.set(false);
    final Entity1 entity4 = new Entity1("456", 4);
    store.write(entity4.id, entity4, 1, interest);
    final Entity1 entity5 = new Entity1("567", 5);
    store.write(entity5.id, entity5, 1, interest);
    dispatcher.processDispatch.set(true);
    dispatcher.control.dispatchUnconfirmed();

    dispatcher.until.completes();
    interest.until.completes();

    assertEquals(5, dispatcher.dispatched.size());
    assertEquals(7, interest.confirmDispatchedResultedIn.get());
    assertEquals("456", dispatcher.dispatched.get(dispatchId("456")).id);
    assertEquals("567", dispatcher.dispatched.get(dispatchId("567")).id);
  }

  @Test
  public void testThatReadErrorIsReported() {
    interest.until = TestUntil.happenings(3); // includes write, confirmation, read (not necessarily in that order)
    final Entity1 entity = new Entity1("123", 1);
    store.write(entity.id, entity, 1, interest);
    store.read(null, Entity1.class, interest);
    interest.until.completes();
    assertEquals(1, interest.errorCauses.size());
    assertEquals("The id is null.", interest.errorCauses.poll().getMessage());
    assertTrue(interest.textReadResult.get().isError());
    assertNull(interest.stateHolder.get());
    
    interest.until = TestUntil.happenings(1);
    store.read(entity.id, null, interest);  // includes read
    interest.until.completes();
    assertEquals(1, interest.errorCauses.size());
    assertEquals("The type is null.", interest.errorCauses.poll().getMessage());
    assertTrue(interest.textReadResult.get().isError());
    assertNull(interest.stateHolder.get());
  }

  @Test
  public void testThatWriteErrorIsReported() {
    interest.until = TestUntil.happenings(1);
    store.write(null, null, 0, interest);
    interest.until.completes();
    assertEquals(1, interest.errorCauses.size());
    assertEquals("The state is null.", interest.errorCauses.poll().getMessage());
    assertTrue(interest.textWriteAccumulatedResults.poll().isError());
    assertNull(interest.stateHolder.get());
  }

  @Before
  public void setUp() throws Exception {
    System.out.println("Starting: " + getClass().getSimpleName() + ": setUp()");

    world = World.startWithDefaults("test-store");

    entity1StoreName = Entity1.class.getSimpleName();
    StateTypeStateStoreMap.stateTypeToStoreName(Entity1.class, entity1StoreName);

    configuration = testConfiguration(DataFormat.Text);

    delegate = delegate();

    interest = new MockResultInterest(0);
    dispatcher = new MockTextDispatcher(0, interest);

    store = world.actorFor(
            Definition.has(JDBCStateStoreActor.class, Definition.parameters(dispatcher, delegate)),
            StateStore.class);
    store.registerAdapter(Entity1.class, new Entity1StateAdapter());
  }

  @After
  public void tearDown() throws Exception {
    System.out.println("Ending: " + getClass().getSimpleName() + ": tearDown()");

    if (configuration == null) return;
    configuration.cleanUp();
    delegate.close();
    world.terminate();
  }

  protected abstract StorageDelegate delegate() throws Exception;
  protected abstract TestConfiguration testConfiguration(final DataFormat format) throws Exception;

  private String dispatchId(final String entityId) {
    return entity1StoreName + ":" + entityId;
  }
}
