package io.vlingo.symbio.store.state.jdbc.postgres.eventjournal;

import io.vlingo.actors.Definition;
import io.vlingo.actors.testkit.TestUntil;
import io.vlingo.symbio.Event;
import io.vlingo.symbio.store.eventjournal.EventJournalReader;
import io.vlingo.symbio.store.eventjournal.EventStream;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static io.vlingo.symbio.store.eventjournal.EventJournalReader.Beginning;
import static io.vlingo.symbio.store.eventjournal.EventJournalReader.End;
import static org.junit.Assert.assertEquals;

public class PostgresEventJournalReaderActorTest extends BasePostgresEventJournalTest {
    private String readerName;

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() {
        readerName = UUID.randomUUID().toString();
    }

    @Test
    public void testThatReturnsCorrectName() {
        String name = journalReader().name().await();
        assertEquals(readerName, name);
    }

    @Test
    public void testThatRetrievesNextEvents() throws Exception {
        EventJournalReader<String> journalReader = journalReader();

        insertEvent(1);
        insertEvent(2);

        assertEquals(1, parse(journalReader.readNext().await()).number);
        assertEquals(2, parse(journalReader.readNext().await()).number);
    }

    @Test
    public void testThatRetrievesFromSavedOffset() throws Exception {
        insertEvent(1);
        insertEvent(2);
        insertEvent(3);
        insertEvent(4);

        insertOffset(3, readerName);
        EventJournalReader<String> journalReader = journalReader();

        assertEquals(3, parse(journalReader.readNext().await()).number);
        assertEquals(4, parse(journalReader.readNext().await()).number);
    }

    @Test
    public void testThatRetrievesInBatches() throws Exception {
        insertEvent(1);
        insertEvent(2);
        insertEvent(3);
        insertEvent(4);

        EventJournalReader<String> journalReader = journalReader();
        EventStream<String> events = journalReader.readNext(2).await();
        assertEquals(2, events.events.size());
        assertEquals(1, parse(events.events.get(0)).number);
        assertEquals(2, parse(events.events.get(1)).number);

        events = journalReader.readNext(2).await();
        assertEquals(2, events.events.size());
        assertEquals(3, parse(events.events.get(0)).number);
        assertEquals(4, parse(events.events.get(1)).number);
    }

    @Test
    public void testThatRewindReadsFromTheBeginning() throws Exception {
        TestUntil until = TestUntil.happenings(1);
        insertEvent(1);
        insertEvent(2);

        insertOffset(3, readerName);
        EventJournalReader<String> journalReader = journalReader();
        journalReader.rewind();

        until.completesWithin(50);
        assertOffsetIs(readerName, 1);
        Event<String> event = journalReader.readNext().await();
        assertEquals(1, parse(event).number);
    }

    @Test
    public void testThatSeekToGoesToTheBeginningWhenSpecified() throws Exception {
        TestUntil until = TestUntil.happenings(1);
        EventJournalReader<String> journalReader = journalReader();
        journalReader.seekTo(Beginning).await();

        until.completesWithin(50);
        assertOffsetIs(readerName, 1);
    }

    @Test
    public void testThatSeekToGoesToTheEndWhenSpecified() throws Exception {
        TestUntil until = TestUntil.happenings(1);
        insertEvent(1);
        insertEvent(2);
        insertEvent(3);

        EventJournalReader<String> journalReader = journalReader();
        journalReader.seekTo(End).await();

        until.completesWithin(50);
        assertOffsetIs(readerName, 4);
    }

    @SuppressWarnings("unchecked")
    private EventJournalReader<String> journalReader() {
        return world.actorFor(
                Definition.has(PostgresEventJournalReaderActor.class,
                        Definition.parameters(configuration, readerName)),
                EventJournalReader.class
        );
    }
}