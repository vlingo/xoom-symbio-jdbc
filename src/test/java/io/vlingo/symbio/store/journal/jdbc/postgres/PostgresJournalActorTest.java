// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.postgres;

import io.vlingo.actors.testkit.AccessSafely;
import io.vlingo.common.Completes;
import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.BaseEntry.TextEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.EntryAdapterProvider;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.StateAdapter;
import io.vlingo.symbio.StateAdapterProvider;
import io.vlingo.symbio.store.common.TestEvent;
import io.vlingo.symbio.store.common.TestEventAdapter;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.journal.Journal;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.journal.Stream;
import io.vlingo.symbio.store.journal.StreamReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PostgresJournalActorTest extends BasePostgresJournalTest {
    private Entity1Adapter entity1Adapter = new Entity1Adapter();
    private Object object = new Object();
    private MockAppendResultInterest interest;
    private Journal<String> journal;
    private MockJournalDispatcher dispatcher;
    private JournalReader<TextEntry> journalReader;
    private StreamReader<String> streamReader;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        interest = new MockAppendResultInterest();

        dispatcher = new MockJournalDispatcher();

        journal =  world.stage().actorFor(Journal.class, PostgresJournalActor.class, dispatcher, configuration);
        EntryAdapterProvider.instance(world).registerAdapter(TestEvent.class, new TestEventAdapter());
        StateAdapterProvider.instance(world).registerAdapter(Entity1.class, entity1Adapter);

        final Completes<JournalReader<TextEntry>> completesJournalReader = journal.journalReader(streamName);
        journalReader = completesJournalReader.await();
        streamReader = journal.streamReader(streamName).await();
    }

    @Test
    public void testThatInsertsANewEvent() {
        dispatcher.afterCompleting(1);

        TestEvent appendedEvent = newEventForData(1);
        journal.append(streamName, 1, appendedEvent, interest, object);

        final Map<String, Dispatchable<Entry<String>, TextState>> dispatched = dispatcher.getDispatched();

        assertEquals(1, dispatched.size());

        Entry<String> entry = journalReader.readNext().await();
        TestEvent event = gson.fromJson(entry.entryData(), TestEvent.class);
        assertEquals(appendedEvent, event);
    }

    @Test
    public void testThatInsertsANewListOfEvents() {
        dispatcher.afterCompleting(1);

        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent2 = newEventForData(2);
        journal.appendAll(streamName, 1, asList(appendedEvent1, appendedEvent2), interest, object);

        final Map<String, Dispatchable<Entry<String>, TextState>> dispatched = dispatcher.getDispatched();
        assertEquals(1, dispatched.size());

        List<TextEntry> eventStream = journalReader.readNext(2).await();
        Entry<String> entry1 = eventStream.get(0);
        TestEvent event1 = gson.fromJson(entry1.entryData(), TestEvent.class);
        assertEquals(appendedEvent1, event1);

        Entry<String> entry2 = eventStream.get(1);
        TestEvent event2 = gson.fromJson(entry2.entryData(), TestEvent.class);
        assertEquals(appendedEvent2, event2);
    }

    @Test
    public void testThatInsertsANewEventWithASnapshot() {
        dispatcher.afterCompleting(3);

        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent2 = newEventForData(2);
        TestEvent appendedEvent3 = newEventForData(3);
        Entity1 entity = new Entity1("1", 123);

        journal.appendWith(streamName, 1, appendedEvent1, null, interest, object);
        journal.appendWith(streamName, 2, appendedEvent2, null, interest, object);
        journal.appendWith(streamName, 3, appendedEvent3, entity, interest, object);

        final Map<String, Dispatchable<Entry<String>, TextState>> dispatched = dispatcher.getDispatched();
        assertEquals(3, dispatched.size());

        Stream<String> eventStream = streamReader.streamFor(streamName, 1).await();
        Entity1 readEntity = entity1Adapter.fromRawState((TextState) eventStream.snapshot);
        assertEquals(entity.id, readEntity.id);
        assertEquals(entity.number, readEntity.number);
    }

    @Test
    public void testThatInsertsANewListOfEventsWithASnapshot() {
        dispatcher.afterCompleting(2);
        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent2 = newEventForData(2);
        TestEvent appendedEvent3 = newEventForData(3);
        TestEvent appendedEvent4 = newEventForData(4);
        Entity1 entity = new Entity1("1", 123);

        journal.appendAllWith(streamName, 1, Arrays.asList(appendedEvent1, appendedEvent2), null, interest, object);
        journal.appendAllWith(streamName, 3, Arrays.asList(appendedEvent3, appendedEvent4), entity, interest, object);

        final Map<String, Dispatchable<Entry<String>, TextState>> dispatched = dispatcher.getDispatched();
        assertEquals(2, dispatched.size());

        Stream<String> eventStream = streamReader.streamFor(streamName, 1).await();
        Entity1 readEntity = entity1Adapter.fromRawState((TextState) eventStream.snapshot);
        assertEquals(entity.id, readEntity.id);
        assertEquals(entity.number, readEntity.number);
    }

    @Test
    public void testThatInsertsANewListOfEventsWithErrorWithoutASnapshot() {
        dispatcher.afterCompleting(0);
        System.out.println("========== BEGIN: testThatInsertsANewListOfEventsWithErrorWithoutASnapshot()");
        System.out.println("========== BEGIN: EXPECTED EXCEPTIONS AHEAD");
        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent3 = newEventForData(3);

        final AccessSafely access = interest.afterCompleting(2);

        journal.appendAllWith(streamName, 1, Arrays.asList(appendedEvent1, null), null, interest, object);
        journal.appendAllWith(streamName, 3, Arrays.asList(appendedEvent3, null), null, interest, object);

        final Map<String, Dispatchable<Entry<String>, TextState>> dispatched = dispatcher.getDispatched();
        assertEquals(0, dispatched.size());

        assertEquals(0, (int) access.readFrom("successCount"));
        assertEquals(2, (int) access.readFrom("failureCount"));
    }

    @Test
    public void testThatReturnsSameReaderForSameName() {
        final String name = UUID.randomUUID().toString();

        final JournalReader<Entry<String>> journalReader1 = journal.journalReader(name).await();
        final JournalReader<Entry<String>> journalReader2 = journal.journalReader(name).await();
        assertEquals(journalReader1, journalReader2);

        final StreamReader<String> streamReader1 = journal.streamReader(name).await();
        final StreamReader<String> streamReader2 = journal.streamReader(name).await();
        assertEquals(streamReader1, streamReader2);
    }

    @Test
    public void testRedispatch() {
        interest.afterCompleting(2);
        final AccessSafely accessDispatcher = dispatcher.afterCompleting(4);

        accessDispatcher.writeUsing("processDispatch", false);

        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent2 = newEventForData(2);
        TestEvent appendedEvent3 = newEventForData(3);
        TestEvent appendedEvent4 = newEventForData(4);
        Entity1 entity = new Entity1("1", 123);

        journal.appendAllWith(streamName, 1, Arrays.asList(appendedEvent1, appendedEvent2), null, interest, object);
        journal.appendAllWith(streamName, 3, Arrays.asList(appendedEvent3, appendedEvent4), entity, interest, object);


        try {
          Thread.sleep(3000);
        } catch (InterruptedException ex) {
          //ignored
        }

        accessDispatcher.writeUsing("processDispatch", true);

        final Map<String, Dispatchable<Entry<String>, TextState>> dispatched = dispatcher.getDispatched();
        assertEquals(2, dispatched.size());

        final int dispatchAttemptCount = accessDispatcher.readFrom("dispatchAttemptCount");
        assertTrue("dispatchAttemptCount", dispatchAttemptCount > 3);

        for (final Dispatchable<Entry<String>, TextState> dispatchable : dispatched.values()) {
          Assert.assertNotNull(dispatchable.createdOn());
          Assert.assertNotNull(dispatchable.id());
          final Collection<Entry<String>> dispatchedEntries = dispatchable.entries();
          Assert.assertEquals(2, dispatchedEntries.size());
        }
    }


    private TestEvent newEventForData(int number) {
          final TestEvent event = new TestEvent(String.valueOf(number), number);
          return event;
      }

      public static final class Entity1 {
        public final String id;
        public final int number;

        public Entity1(final String id, final int number) {
          this.id = id;
          this.number = number;
        }
      }

      public static final class Entity1Adapter implements StateAdapter<Entity1,TextState> {

        @Override
        public int typeVersion() {
          return 1;
        }

        @Override
        public Entity1 fromRawState(TextState raw) {
          return JsonSerialization.deserialized(raw.data, raw.typed());
        }

        @Override
        public <ST> ST fromRawState(final TextState raw, final Class<ST> stateType) {
          return JsonSerialization.deserialized(raw.data, stateType);
        }

        @Override
        public TextState toRawState(Entity1 state, int stateVersion, Metadata metadata) {
          final String serialization = JsonSerialization.serialized(state);
          return new TextState(state.id, Entity1.class, typeVersion(), serialization, stateVersion, metadata);
        }

        @Override
        public TextState toRawState(final String id, final Entity1 state, final int stateVersion, final Metadata metadata) {
          final String serialization = JsonSerialization.serialized(state);
          return new TextState(id, Entity1.class, typeVersion(), serialization, stateVersion, metadata);
        }
      }
  }