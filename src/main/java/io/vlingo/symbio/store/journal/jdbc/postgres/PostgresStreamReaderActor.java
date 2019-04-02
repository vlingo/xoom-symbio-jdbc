// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.postgres;

import com.google.gson.Gson;
import io.vlingo.actors.Actor;
import io.vlingo.common.Completes;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.State.TextState;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.journal.Stream;
import io.vlingo.symbio.store.journal.StreamReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;

public class PostgresStreamReaderActor extends Actor implements StreamReader<String> {
    private static final String QUERY_EVENTS =
            "SELECT id, entry_data, entry_metadata, entry_type, entry_type_version " +
                    "FROM vlingo_symbio_journal " +
                    "WHERE stream_name = ? AND stream_version >= ?";

    private static final String QUERY_SNAPSHOT =
            "SELECT snapshot_type, snapshot_type_version, snapshot_data, snapshot_data_version, snapshot_metadata " +
                    "FROM vlingo_symbio_journal_snapshots " +
                    "WHERE stream_name = ?";

    private final Connection connection;
    private final PreparedStatement queryEventsStatement;
    private final PreparedStatement queryLatestSnapshotStatement;
    private final Gson gson;

    public PostgresStreamReaderActor(final Configuration configuration) throws SQLException {
        this.connection = configuration.connection;
        this.queryEventsStatement = this.connection.prepareStatement(QUERY_EVENTS);
        this.queryLatestSnapshotStatement = this.connection.prepareStatement(QUERY_SNAPSHOT);
        this.gson = new Gson();
    }

    @Override
    public Completes<Stream<String>> streamFor(final String streamName) {
        return streamFor(streamName, 1);
    }

    @Override
    public Completes<Stream<String>> streamFor(final String streamName, final int fromStreamVersion) {
        try {
            return completes().with(eventsFromOffset(streamName, fromStreamVersion));
        } catch (Exception e) {
            logger().log("vlingo/symbio-postgresql: " + e.getMessage(), e);
            return completes().with(new Stream<>(streamName, 1, emptyList(), TextState.Null));
        }
    }

    private Stream<String> eventsFromOffset(final String streamName, final int offset) throws Exception {
        final State<String> snapshot = latestSnapshotOf(streamName);
        final List<BaseEntry<String>> events = new ArrayList<>();

        int dataVersion = offset;
        State<String> referenceSnapshot = TextState.Null;

        if (snapshot != TextState.Null) {
            if (snapshot.dataVersion > offset) {
                dataVersion = snapshot.dataVersion;
                referenceSnapshot = snapshot;
            }
        }

        queryEventsStatement.setString(1, streamName);
        queryEventsStatement.setInt(2, dataVersion);
        final ResultSet resultSet = queryEventsStatement.executeQuery();
        while (resultSet.next()) {
            final String id = resultSet.getString(1);
            final String eventData = resultSet.getString(2);
            final String eventMetadata = resultSet.getString(3);
            final String eventType = resultSet.getString(4);
            final int eventTypeVersion = resultSet.getInt(5);

            final Class<?> classOfEvent = Class.forName(eventType);
            final Metadata eventMetadataDeserialized = gson.fromJson(eventMetadata, Metadata.class);

            events.add(new BaseEntry.TextEntry(id, classOfEvent, eventTypeVersion, eventData, eventMetadataDeserialized));
        }

        return new Stream<>(streamName, dataVersion + events.size(), events, referenceSnapshot);
    }

    private State<String> latestSnapshotOf(final String streamName) throws Exception {
        queryLatestSnapshotStatement.setString(1, streamName);
        final ResultSet resultSet = queryLatestSnapshotStatement.executeQuery();

        if (resultSet.next()) {
            final String snapshotDataType = resultSet.getString(1);
            final int snapshotTypeVersion = resultSet.getInt(2);
            final String snapshotData = resultSet.getString(3);
            final int snapshotDataVersion = resultSet.getInt(4);
            final String metadataJson = resultSet.getString(5);

            final Class<?> classOfEvent = Class.forName(snapshotDataType);
            final Metadata eventMetadataDeserialized = gson.fromJson(metadataJson, Metadata.class);

            return new State.TextState(streamName, classOfEvent, snapshotTypeVersion, snapshotData, snapshotDataVersion, eventMetadataDeserialized);
        }

        return TextState.Null;
    }
}
