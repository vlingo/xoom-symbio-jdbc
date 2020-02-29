// Copyright © 2012-2020 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.vlingo.actors.Logger;
import io.vlingo.common.serialization.JsonSerialization;
import io.vlingo.symbio.BaseEntry;
import io.vlingo.symbio.Entry;
import io.vlingo.symbio.Metadata;
import io.vlingo.symbio.State;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.DatabaseType;
import io.vlingo.symbio.store.dispatch.Dispatchable;
import io.vlingo.symbio.store.dispatch.DispatcherControl;

public class JDBCDispatcherControlDelegate implements DispatcherControl.DispatcherControlDelegate<Entry<String>, State.TextState> {
    static final String DISPATCHEABLE_ENTRIES_DELIMITER = "|";

    private final Connection connection;
    private final DatabaseType databaseType;
    private final Logger logger;
    private final PreparedStatement selectDispatchables;
    private final JDBCQueries queries;

    public JDBCDispatcherControlDelegate(final Configuration configuration, final Logger logger) throws SQLException {
        this.connection = configuration.connection;
        this.databaseType = configuration.databaseType;
        this.logger = logger;
        this.queries = JDBCQueries.queriesFor(configuration.connection);

        queries.createTables();

        this.selectDispatchables = queries.prepareSelectDispatchablesQuery(configuration.originatorId);
    }

    @Override
    public Collection<Dispatchable<Entry<String>, State.TextState>> allUnconfirmedDispatchableStates() throws Exception {
        final List<Dispatchable<Entry<String>, State.TextState>> dispatchables = new ArrayList<>();

        try (final ResultSet result = selectDispatchables.executeQuery()) {
            while (result.next()) {
                dispatchables.add(dispatchableFrom(result));
            }
        }

        return dispatchables;
    }

    @Override
    public void confirmDispatched(final String dispatchId) {
        try {
            queries.prepareDeleteDispatchableQuery(dispatchId).executeUpdate();
            doCommit();
        } catch (final Exception e) {
            logger.error("vlingo/symbio-jdbc-" + databaseType + ": Failed to confirm dispatch with id" + dispatchId, e);
            fail();
        }
    }

    @Override
    public void stop() {
        try {
            queries.close();
        } catch (final SQLException e) {
            //ignore
        }
    }

    private void doCommit() {
        try {
            connection.commit();
        } catch (final SQLException e) {
            logger.error("vlingo/symbio-jdbc-" + databaseType + ": Could not complete transaction", e);
            throw new IllegalStateException(e);
        }
    }

    private void fail() {
        try {
            connection.rollback();
        } catch (final Exception e) {
            logger.error(getClass().getSimpleName() + ": Rollback failed because: " + e.getMessage(), e);
        }
    }

    private Dispatchable<Entry<String>, State.TextState> dispatchableFrom(final ResultSet resultSet) throws SQLException, ClassNotFoundException {

        final String dispatchId = resultSet.getString(1);

        LocalDateTime createdOn =
                LocalDateTime.ofInstant(
                        Instant.ofEpochMilli(resultSet.getLong(2)),
                        ZoneId.systemDefault());

        final State.TextState state;

        final String stateId = resultSet.getString(3);

        if (stateId != null && !stateId.isEmpty()) {
            final String data = resultSet.getString(4);
            final int dataVersion = resultSet.getInt(5);
            final Class<?> type = Class.forName(resultSet.getString(6));
            final int typeVersion = resultSet.getInt(7);
            final String metadataValue = resultSet.getString(8);
            final Metadata metadata = JsonSerialization.deserialized(metadataValue, Metadata.class);

            state = new State.TextState(stateId, type, typeVersion, data, dataVersion, metadata);
        } else {
            state = null;
        }

        final String entriesIds = resultSet.getString(9);
        final List<Entry<String>> entries = new ArrayList<>();
        if (entriesIds != null && !entriesIds.isEmpty()) {
            final String[] ids = entriesIds.split("\\" + DISPATCHEABLE_ENTRIES_DELIMITER);
            for (final String entryId : ids) {
                try (final ResultSet result = queries.prepareSelectEntryQuery(Long.parseLong(entryId)).executeQuery()) {
                    if (result.next()) {
                        entries.add(entryFrom(result));
                    }
                }
            }
        }
        return new Dispatchable<>(dispatchId, createdOn, state, entries);
    }

    private Entry<String> entryFrom(final ResultSet resultSet) throws SQLException, ClassNotFoundException {
        final String id = resultSet.getString(1);
        final String entryData = resultSet.getString(2);
        final String entryType = resultSet.getString(3);
        final int eventTypeVersion = resultSet.getInt(4);
        final String entryMetadata = resultSet.getString(5);
        final int entryVerion = resultSet.getInt(6); // from E_STREAM_VERSION

        final Class<?> classOfEvent = Class.forName(entryType);

        final Metadata metadata = JsonSerialization.deserialized(entryMetadata, Metadata.class);
        return new BaseEntry.TextEntry(id, classOfEvent, eventTypeVersion, entryData, entryVerion, metadata);
    }
}
