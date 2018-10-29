package io.vlingo.symbio.store.state.jdbc.postgres.eventjournal;

import io.vlingo.actors.Definition;
import io.vlingo.actors.testkit.TestUntil;
import io.vlingo.symbio.Event;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.eventjournal.*;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.UUID;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;

public class PostgresEventJournalActorTest extends BasePostgresEventJournalTest {
    private EventJournal<String> journal;
    private EventJournalListener<String> listener;
    private EventJournalReader<String> journalReader;
    private EventStreamReader<String> streamReader;
    private TestUntil until;

    @Before
    public void setUp() throws Exception {
        until = TestUntil.happenings(1);
        listener = Mockito.mock(EventJournalListener.class);
        journal = world.actorFor(
                Definition.has(
                        PostgresEventJournalActor.class,
                        Definition.parameters(configuration, listener)
                ),
                EventJournal.class
        );

        Mockito.doAnswer(x -> until.happened()).when(listener).appended(any());
        Mockito.doAnswer(x -> until.happened()).when(listener).appendedAll(any());
        Mockito.doAnswer(x -> until.happened()).when(listener).appendedWith(any(), any());
        Mockito.doAnswer(x -> until.happened()).when(listener).appendedAllWith(any(), any());

        journalReader = journal.eventJournalReader(streamName).await();
        streamReader = journal.eventStreamReader(streamName).await();
    }

    @Test
    public void testThatInsertsANewEvent() {
        Event<String> appendedEvent = newEventForData(1);
        journal.append(streamName, 1, appendedEvent);
        until.completes();

        Event<String> event = journalReader.readNext().await();
        assertEquals(appendedEvent, event);
        assertEquals(appendedEvent.eventData, event.eventData.replace(" ", ""));
    }

    @Test
    public void testThatInsertsANewListOfEvents() {
        Event<String> appendedEvent1 = newEventForData(1);
        Event<String> appendedEvent2 = newEventForData(2);
        journal.appendAll(streamName, 1, asList(appendedEvent1, appendedEvent2));
        until.completes();

        EventStream<String> eventStream = journalReader.readNext(2).await();
        assertEquals(appendedEvent1, eventStream.events.get(0));
        assertEquals(appendedEvent1.eventData, eventStream.events.get(0).eventData.replace(" ", ""));

        assertEquals(appendedEvent2, eventStream.events.get(1));
        assertEquals(appendedEvent2.eventData, eventStream.events.get(1).eventData.replace(" ", ""));
    }

    @Test
    public void testThatInsertsANewEventWithASnapshot() {
        Event<String> appendedEvent = newEventForData(1);
        State<String> snapshot = newSnapshotForData(2);

        journal.appendWith(streamName, 1, appendedEvent, snapshot);
        until.completes();

        EventStream<String> eventStream = streamReader.streamFor(streamName, 1).await();
        assertEquals(snapshot.data, eventStream.snapshot.data);
    }

    @Test
    public void testThatInsertsANewListOfEventsWithASnapshot() {
        Event<String> appendedEvent1 = newEventForData(1);
        Event<String> appendedEvent2 = newEventForData(2);
        State<String> snapshot = newSnapshotForData(2);

        journal.appendAllWith(streamName, 1, Arrays.asList(appendedEvent1, appendedEvent2), snapshot);
        until.completes();

        EventStream<String> eventStream = streamReader.streamFor(streamName, 1).await();
        assertEquals(snapshot.data, eventStream.snapshot.data);
    }

    @Test
    public void testThatReturnsSameReaderForSameName() {
        final String name = UUID.randomUUID().toString();
        assertEquals(journal.eventJournalReader(name).await(), journal.eventJournalReader(name).await());
        assertEquals(journal.eventStreamReader(name).await(), journal.eventStreamReader(name).await());
    }

    private Event<String> newEventForData(int number) {
        final TestEvent event = new TestEvent(String.valueOf(number), number);
        return new Event.TextEvent(String.valueOf(number), TestEvent.class, 1, gson.toJson(event), new Metadata());
    }

    private State<String> newSnapshotForData(int number) {
        return new State.TextState(String.valueOf(number), TestEvent.class, 1, String.valueOf(number), number);
    }
}