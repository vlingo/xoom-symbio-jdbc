// Copyright © 2012-2023 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.jdbc;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.xoom.actors.Definition;
import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.actors.testkit.AccessSafely;
import io.vlingo.xoom.symbio.BaseEntry;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.EntryAdapterProvider;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.StateAdapterProvider;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.TestEvents;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.dispatch.Dispatchable;
import io.vlingo.xoom.symbio.store.dispatch.Dispatcher;
import io.vlingo.xoom.symbio.store.dispatch.DispatcherControl;
import io.vlingo.xoom.symbio.store.dispatch.control.DispatcherControlActor;
import io.vlingo.xoom.symbio.store.state.Entity1;
import io.vlingo.xoom.symbio.store.state.MockResultInterest;
import io.vlingo.xoom.symbio.store.state.MockTextDispatcher;
import io.vlingo.xoom.symbio.store.state.StateStore;
import io.vlingo.xoom.symbio.store.state.StateTypeStateStoreMap;

public abstract class JDBCTextStateStoreEntryTest {
    private Configuration.TestConfiguration configuration;
    private StateStore.StorageDelegate delegate;
    private MockTextDispatcher dispatcher;
    private EntryAdapterProvider entryAdapterProvider;
    private String entity1StoreName;
    private MockResultInterest interest;
    private StateStore store;
    private World world;

    @Test
    public void testThatSourcesAppendAsEntries() {
        final AccessSafely accessInterest1 = interest.afterCompleting(3);
        dispatcher.afterCompleting(0);

        final Entity1 entity1 = new Entity1("123", 1);
        store.write(entity1.id, entity1, 1, Arrays.asList(new TestEvents.Event1()), interest);

        final Entity1 entity2 = new Entity1("234", 2);
        store.write(entity2.id, entity2, 1, Arrays.asList(new TestEvents.Event2()), interest);

        final Entity1 entity3 = new Entity1("345", 3);
        store.write(entity3.id, entity3, 1, Arrays.asList(new TestEvents.Event3()), interest);

        assertEquals(3, (int) accessInterest1.readFrom("textWriteAccumulatedSourcesCount"));

        final List<BaseEntry.TextEntry> readEntries = new ArrayList<>();
        final AccessSafely accessReadEntries = AccessSafely.afterCompleting(1);
        accessReadEntries
                .writingWith("all", (List<BaseEntry.TextEntry> all) -> readEntries.addAll(all))
                .readingWith("all", () -> readEntries)
                .readingWith("allCount", () -> readEntries.size());

        store.entryReader("test")
                .andThenTo(reader -> reader.readNext(3))
                .andThenConsume((List<Entry<?>> all) -> {
                    accessReadEntries.writeUsing("all", all);
                });

        assertEquals(3, (int) accessReadEntries.readFrom("allCount"));

        assertEquals(new TestEvents.Event1(), entryAdapterProvider.asSource(readEntries.get(0)));
        assertEquals(new TestEvents.Event2(), entryAdapterProvider.asSource(readEntries.get(1)));
        assertEquals(new TestEvents.Event3(), entryAdapterProvider.asSource(readEntries.get(2)));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Before
    public void setUp() throws Exception {
        world = World.startWithDefaults("test-store");

        entity1StoreName = Entity1.class.getSimpleName();
        StateTypeStateStoreMap.stateTypeToStoreName(Entity1.class, entity1StoreName);

        configuration = testConfiguration(DataFormat.Text);
        delegate = storageDelegate(configuration, world.defaultLogger());
        interest = new MockResultInterest(false);
        dispatcher = new MockTextDispatcher(0, interest);

        final StateAdapterProvider stateAdapterProvider = new StateAdapterProvider(world);
        entryAdapterProvider = new EntryAdapterProvider(world);

        stateAdapterProvider.registerAdapter(Entity1.class, new Entity1.Entity1StateAdapter());
        // NOTE: No adapter registered for Entity2.class because it will use the default

        DispatcherControl dispatcherControl = world.stage().actorFor(DispatcherControl.class,
                Definition.has(DispatcherControlActor.class,
                        new DispatcherControl.DispatcherControlInstantiator(typed(dispatcher), typed(delegate),
                                StateStore.DefaultCheckConfirmationExpirationInterval, StateStore.DefaultConfirmationExpiration)));

        JDBCEntriesWriter entriesWriter = new JDBCEntriesInstantWriter(typed(delegate), Arrays.asList(typed(dispatcher)), dispatcherControl);
        store = world.actorFor(StateStore.class, JDBCStateStoreActor.class, delegate, entriesWriter);
    }

    @After
    public void tearDown() throws Exception {
        if (configuration == null) return;
        world.terminate();
        delegate.close();
        configuration.cleanUp();
    }

    /**
     * Create specific storage delegate.
     * @param configuration
     * @param logger
     * @return
     */
    protected abstract StateStore.StorageDelegate storageDelegate(Configuration.TestConfiguration configuration, final Logger logger);

    /**
     * Create specific test configuration.
     * @param format
     * @return
     * @throws Exception
     */
    protected abstract Configuration.TestConfiguration testConfiguration(final DataFormat format) throws Exception;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Dispatcher<Dispatchable<? extends Entry<?>, ? extends State<?>>> typed(Dispatcher dispatcher) {
        return dispatcher;
    }

    @SuppressWarnings("rawtypes")
    private JDBCStorageDelegate typed(StateStore.StorageDelegate delegate) {
        return (JDBCStorageDelegate)delegate;
    }
}
