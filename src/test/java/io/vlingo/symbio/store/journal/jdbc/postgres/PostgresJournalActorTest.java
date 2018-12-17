package io.vlingo.symbio.store.journal.jdbc.postgres;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

import java.util.Arrays;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.vlingo.actors.Definition;
import io.vlingo.actors.testkit.TestUntil;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.journal.Journal;
import io.vlingo.symbio.store.journal.JournalListener;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.journal.Stream;
import io.vlingo.symbio.store.journal.StreamReader;

public class PostgresJournalActorTest extends BasePostgresJournalTest {
    private Object object = new Object();
    private MockAppendResultInterest interest;
    private Journal<String> journal;
    private JournalListener<String> listener;
    private JournalReader<String> journalReader;
    private StreamReader<String> streamReader;
    private TestUntil until;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws Exception {
        interest = new MockAppendResultInterest();
        until = TestUntil.happenings(1);
        listener = Mockito.mock(JournalListener.class);
        journal = world.actorFor(
                Definition.has(
                        PostgresJournalActor.class,
                        Definition.parameters(configuration, listener)
                ),
                Journal.class
        );
        journal.registerAdapter(TestEvent.class, new TestEventAdapter());

        Mockito.doAnswer(x -> until.happened()).when(listener).appended(any());
        Mockito.doAnswer(x -> until.happened()).when(listener).appendedAll(any());
        Mockito.doAnswer(x -> until.happened()).when(listener).appendedWith(any(), any());
        Mockito.doAnswer(x -> until.happened()).when(listener).appendedAllWith(any(), any());

        journalReader = journal.journalReader(streamName).await();
        streamReader = journal.streamReader(streamName).await();
    }

    @Test
    public void testThatInsertsANewEvent() {
        TestEvent appendedEvent = newEventForData(1);
        journal.append(streamName, 1, appendedEvent, interest, object);
        until.completes();

        Entry<String> entry = journalReader.readNext().await();
        TestEvent event = gson.fromJson(entry.entryData, TestEvent.class);
        assertEquals(appendedEvent, event);
    }

    @Test
    public void testThatInsertsANewListOfEvents() {
        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent2 = newEventForData(2);
        journal.appendAll(streamName, 1, asList(appendedEvent1, appendedEvent2), interest, object);
        until.completes();

        Stream<String> eventStream = journalReader.readNext(2).await();
        Entry<String> entry1 = eventStream.entries.get(0);
        TestEvent event1 = gson.fromJson(entry1.entryData, TestEvent.class);
        assertEquals(appendedEvent1, event1);

        Entry<String> entry2 = eventStream.entries.get(1);
        TestEvent event2 = gson.fromJson(entry2.entryData, TestEvent.class);
        assertEquals(appendedEvent2, event2);
    }

    @Test
    public void testThatInsertsANewEventWithASnapshot() {
        TestEvent appendedEvent = newEventForData(1);
        State<String> snapshot = newSnapshotForData(2);

        journal.appendWith(streamName, 1, appendedEvent, snapshot, interest, object);
        until.completes();

        Stream<String> eventStream = streamReader.streamFor(streamName, 1).await();
        assertEquals(snapshot.data, eventStream.snapshot.data);
    }

    @Test
    public void testThatInsertsANewListOfEventsWithASnapshot() {
        TestEvent appendedEvent1 = newEventForData(1);
        TestEvent appendedEvent2 = newEventForData(2);
        State<String> snapshot = newSnapshotForData(2);

        journal.appendAllWith(streamName, 1, Arrays.asList(appendedEvent1, appendedEvent2), snapshot, interest, object);
        until.completes();

        Stream<String> eventStream = streamReader.streamFor(streamName, 1).await();
        assertEquals(snapshot.data, eventStream.snapshot.data);
    }

    @Test
    public void testThatReturnsSameReaderForSameName() {
        final String name = UUID.randomUUID().toString();
        assertEquals(journal.journalReader(name).await(), journal.journalReader(name).await());
        assertEquals(journal.streamReader(name).await(), journal.streamReader(name).await());
    }

    private TestEvent newEventForData(int number) {
        final TestEvent event = new TestEvent(String.valueOf(number), number);
        return event;
    }

    private State<String> newSnapshotForData(int number) {
        return new State.TextState(String.valueOf(number), TestEvent.class, 1, String.valueOf(number), number);
    }
}