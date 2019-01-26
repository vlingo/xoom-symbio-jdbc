// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.postgres;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;

import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.journal.JournalReader;
import io.vlingo.symbio.store.journal.Stream;

public class PostgresJournalReaderActor extends Actor implements JournalReader<String> {
    private static final String QUERY_CURRENT_OFFSET =
            "SELECT reader_offset FROM vlingo_symbio_journal_offsets WHERE reader_name=?";

    private static final String UPDATE_CURRENT_OFFSET =
            "INSERT INTO vlingo_symbio_journal_offsets(reader_offset, reader_name) VALUES(?, ?) " +
                    "ON CONFLICT (reader_name) DO UPDATE SET reader_offset=?";

    private static final String QUERY_SINGLE =
            "SELECT id, entry_data, entry_metadata, entry_type, entry_type_version, entry_timestamp FROM " +
                    "vlingo_symbio_journal WHERE entry_timestamp >= ?";

    private static final String QUERY_BATCH =
            "SELECT id, entry_data, entry_metadata, entry_type, entry_type_version, entry_timestamp FROM " +
                    "vlingo_symbio_journal WHERE entry_timestamp > ?";

    private static final String QUERY_LAST_OFFSET =
            "SELECT MAX(entry_timestamp) FROM vlingo_symbio_journal";

    private final Connection connection;
    private final String name;
    private final PreparedStatement queryCurrentOffset;
    private final PreparedStatement updateCurrentOffset;
    private final PreparedStatement querySingleEvent;
    private final PreparedStatement queryEventBatch;
    private final PreparedStatement queryLastOffset;
    private final Gson gson;

    private long offset;

    public PostgresJournalReaderActor(final Configuration configuration, final String name) throws SQLException {
        this.connection = configuration.connection;
        this.name = name;

        this.queryCurrentOffset = this.connection.prepareStatement(QUERY_CURRENT_OFFSET);
        this.updateCurrentOffset = this.connection.prepareStatement(UPDATE_CURRENT_OFFSET);
        this.querySingleEvent = this.connection.prepareStatement(QUERY_SINGLE);
        this.queryEventBatch = this.connection.prepareStatement(QUERY_BATCH);
        this.queryLastOffset = this.connection.prepareStatement(QUERY_LAST_OFFSET);

        this.gson = new Gson();
        retrieveCurrentOffset();
    }

    @Override
    public Completes<String> name() {
        return completes().with(name);
    }

    @Override
    public Completes<Entry<String>> readNext() {
        try {
            querySingleEvent.setLong(1, offset);
            final ResultSet resultSet = querySingleEvent.executeQuery();
            if (resultSet.next()) {
                offset = nextOffsetFromResultSet(resultSet);
                updateCurrentOffset();
                return completes().with(eventFromResultSet(resultSet));
            }
        } catch (Exception e) {
            logger().log("vlingo/symbio-postgres: " + e.getMessage(), e);
        }

        return completes().with(null);
    }

    @Override
    public Completes<Stream<String>> readNext(int maximumEvents) {
        try {
            List<Entry<String>> events = new ArrayList<>(maximumEvents);
            queryEventBatch.setLong(1, offset);
            queryEventBatch.setMaxRows(maximumEvents);

            final ResultSet resultSet = queryEventBatch.executeQuery();
            while (resultSet.next()) {
                events.add(eventFromResultSet(resultSet));
                if (resultSet.isLast()) {
                    offset = nextOffsetFromResultSet(resultSet);
                }
            }

            updateCurrentOffset();
            return completes().with(new Stream<>(name, (int) offset, events, TextState.Null));

        } catch (Exception e) {
            logger().log("vlingo/symbio-postgres: " + e.getMessage(), e);
        }

        return completes().with(null);
    }

    @Override
    public void rewind() {
        this.offset = 1;
        updateCurrentOffset();
    }

    @Override
    public Completes<String> seekTo(String id) {
        switch (id) {
            case Beginning:
                this.offset = 1;
                updateCurrentOffset();
                break;
            case End:
                this.offset = retrieveLatestOffset() + 1;
                updateCurrentOffset();
                break;
            case Query:
                break;
            default:
                this.offset = Integer.parseInt(id);
                updateCurrentOffset();
                break;
        }

        return completes().with(String.valueOf(offset));
    }


    private Entry<String> eventFromResultSet(ResultSet resultSet) throws SQLException, ClassNotFoundException {
        final String id = resultSet.getString(1);
        final String entryData = resultSet.getString(2);
        final String entryMetadata = resultSet.getString(3);
        final String entryType = resultSet.getString(4);
        final int eventTypeVersion = resultSet.getInt(5);

        final Class<?> classOfEvent = Class.forName(entryType);

        final Metadata eventMetadataDeserialized = gson.fromJson(entryMetadata, Metadata.class);
        return new Entry.TextEntry(id, classOfEvent, eventTypeVersion, entryData, eventMetadataDeserialized);
    }

    private void retrieveCurrentOffset() {
        this.offset = 1;

        try {
            queryCurrentOffset.setString(1, name);
            final ResultSet resultSet = queryCurrentOffset.executeQuery();
            if (resultSet.next()) {
                this.offset = resultSet.getLong(1);
            }
        } catch (Exception e) {
            logger().log("vlingo/symbio-postgres: " + e.getMessage(), e);
            logger().log("vlingo/symbio-postgres: Rewinding the offset");
        }
    }

    private void updateCurrentOffset() {
        try {
            updateCurrentOffset.setLong(1, offset);
            updateCurrentOffset.setString(2, name);
            updateCurrentOffset.setLong(3, offset);

            updateCurrentOffset.executeUpdate();
            connection.commit();
        } catch (Exception e) {
            logger().log("vlingo/symbio-postgres: Could not persist the offset. Will retry on next read.");
            logger().log("vlingo/symbio-postgres: " + e.getMessage(), e);
        }
    }

    private long retrieveLatestOffset() {
        try {
            ResultSet resultSet = queryLastOffset.executeQuery();
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
        } catch (Exception e) {
            logger().log("vlingo/symbio-postgres: Could not retrieve latest offset, using current.");
        }

        return offset;
    }

    private long nextOffsetFromResultSet(ResultSet resultSet) throws SQLException {
        return resultSet.getLong(6) + 1;
    }
}
