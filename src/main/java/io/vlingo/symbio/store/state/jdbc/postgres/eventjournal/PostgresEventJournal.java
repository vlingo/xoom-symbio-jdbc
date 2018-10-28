package io.vlingo.symbio.store.state.jdbc.postgres.eventjournal;

import io.vlingo.actors.Actor;
import io.vlingo.actors.Address;
import io.vlingo.actors.Definition;
import io.vlingo.common.Completes;
import io.vlingo.symbio.Event;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.eventjournal.EventJournal;
import io.vlingo.symbio.store.eventjournal.EventJournalListener;
import io.vlingo.symbio.store.eventjournal.EventJournalReader;
import io.vlingo.symbio.store.eventjournal.EventStreamReader;
import io.vlingo.symbio.store.state.jdbc.Configuration;

import java.sql.Connection;
import java.util.List;

public class PostgresEventJournal<T> extends Actor implements EventJournal<T> {
    private final Configuration configuration;
    private final Connection connection;
    private final EventJournalListener<T> listener;

    public PostgresEventJournal(Configuration configuration, Connection connection, EventJournalListener<T> listener) {
        this.configuration = configuration;
        this.connection = connection;
        this.listener = listener;
    }

    @Override
    public void append(String streamName, int streamVersion, Event<T> event) {
    }

    @Override
    public void appendWith(String streamName, int streamVersion, Event<T> event, State<T> snapshot) {

    }

    @Override
    public void appendAll(String streamName, int fromStreamVersion, List<Event<T>> events) {

    }

    @Override
    public void appendAllWith(String streamName, int fromStreamVersion, List<Event<T>> events, State<T> snapshot) {

    }

    @Override
    @SuppressWarnings("unchecked")
    public Completes<EventJournalReader<T>> eventJournalReader(String name) {
        Address address = stage().world().addressFactory().from("eventJournalReader-" + name);
        EventJournalReader<T>  reader = stage().actorFor(
                Definition.has(
                        PostgresEventJournalReaderActor.class,
                        Definition.parameters(configuration, name)
                ),
                EventJournalReader.class,
                address
        );

        return completes().with(reader);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Completes<EventStreamReader<T>> eventStreamReader(String name) {
        Address address = stage().world().addressFactory().from("eventStreamReader-" + name);
        EventStreamReader<T> reader = stage().actorFor(
                Definition.has(
                        PostgresEventStreamReaderActor.class,
                        Definition.parameters(configuration)),
                EventStreamReader.class,
                address
        );

        return completes().with(reader);
    }
}
