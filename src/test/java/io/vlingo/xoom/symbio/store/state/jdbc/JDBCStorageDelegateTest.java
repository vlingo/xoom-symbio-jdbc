// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.state.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.symbio.Metadata;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.store.DataFormat;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.state.Entity1;
import io.vlingo.xoom.symbio.store.state.StateTypeStateStoreMap;

public abstract class JDBCStorageDelegateTest {
    private Configuration.TestConfiguration configuration;
    private JDBCStorageDelegate<Object> delegate;
    private String entity1StoreName;
    private World world;

    @Test
    public void testThatDatabaseOpensTablesCreated() throws Exception {
        configuration = testConfiguration(DataFormat.Text);
        delegate = storageDelegate(configuration, world.defaultLogger());

        assertNotNull(delegate);
    }

    @Test
    public void testThatTextWritesRead() throws Exception {
        configuration = testConfiguration(DataFormat.Text);
        delegate = storageDelegate(configuration, world.defaultLogger());

        assertNotNull(delegate);

        final State.TextState writeState = new State.TextState("123", Entity1.class, 1, "{ \"data\" : \"data1\" }", 1, Metadata.with("metadata", "op"));

        delegate.beginWrite();
        final PreparedStatement writeStatement = delegate.writeExpressionFor(entity1StoreName, writeState);
        writeStatement.executeUpdate();
        delegate.complete();

        delegate.beginRead();
        final QueryResource queryResource = delegate.createReadExpressionFor(entity1StoreName, "123");
        final ResultSet result = queryResource.execute();
        final State.TextState readState = delegate.stateFrom(result, "123");
        result.close();
        queryResource.close();
        delegate.complete();

        assertEquals(writeState, readState);
    }

    @Test
    public void testThatTextStatesUpdate() throws Exception {
        configuration = testConfiguration(DataFormat.Text);
        delegate = storageDelegate(configuration, world.defaultLogger());

        assertNotNull(delegate);

        final State.TextState writeState1 = new State.TextState("123", Entity1.class, 1, "{ \"data\" : \"data1\" }", 1, Metadata.with("metadata1", "op1"));

        delegate.beginWrite();
        final PreparedStatement writeStatement1 = delegate.writeExpressionFor(entity1StoreName, writeState1);
        writeStatement1.executeUpdate();
        delegate.complete();

        delegate.beginRead();
        final QueryResource queryResource1 = delegate.createReadExpressionFor(entity1StoreName, "123");
        final ResultSet result1 = queryResource1.execute();
        final State.TextState readState1 = delegate.stateFrom(result1, "123");
        result1.close();
        queryResource1.close();
        delegate.complete();

        assertEquals(writeState1, readState1);

        final State.TextState writeState2 = new State.TextState("123", Entity1.class, 1, "{ \"data\" : \"data1\" }", 1, Metadata.with("metadata2", "op2"));

        delegate.beginWrite();
        final PreparedStatement writeStatement2 = delegate.writeExpressionFor(entity1StoreName, writeState2);
        writeStatement2.executeUpdate();
        delegate.complete();

        delegate.beginRead();
        final QueryResource queryResource2 = delegate.createReadExpressionFor(entity1StoreName, "123");
        final ResultSet result2 = queryResource2.execute();
        final State.TextState readState2 = delegate.stateFrom(result2, "123");
        result2.close();
        queryResource2.close();
        delegate.complete();

        assertEquals(writeState2, readState2);
        assertNotEquals(0, writeState1.compareTo(readState2));
        assertNotEquals(0, writeState2.compareTo(readState1));
    }

    @Test
    public void testThatBinaryWritesRead() throws Exception {
        configuration = testConfiguration(DataFormat.Binary);
        delegate = storageDelegate(configuration, world.defaultLogger());

        assertNotNull(delegate);

        final State.BinaryState writeState = new State.BinaryState("123", Entity1.class, 1, "{ \"data\" : \"data1\" }".getBytes(), 1, Metadata.with("metadata", "op"));

        delegate.beginWrite();
        final PreparedStatement writeStatement = delegate.writeExpressionFor(entity1StoreName, writeState);
        writeStatement.executeUpdate();
        delegate.complete();

        delegate.beginRead();
        final QueryResource queryResource = delegate.createReadExpressionFor(entity1StoreName, "123");
        final ResultSet resultSet = queryResource.execute();
        final State.BinaryState readState = delegate.stateFrom(resultSet, "123");
        resultSet.close();
        queryResource.close();
        delegate.complete();

        assertEquals(writeState, readState);
    }

    @Before
    public void setUp() {
        world = World.startWithDefaults("test-store");

        entity1StoreName = Entity1.class.getSimpleName();
        StateTypeStateStoreMap.stateTypeToStoreName(Entity1.class, entity1StoreName);
    }

    @After
    public void tearDown() throws Exception {
        delegate.close();
        Thread.sleep(1000);
        configuration.cleanUp();
        Thread.sleep(1000);
        world.terminate();
    }

    /**
     * Create specific storage delegate.
     * @param configuration
     * @param logger
     * @return
     */
    protected abstract JDBCStorageDelegate<Object> storageDelegate(Configuration.TestConfiguration configuration, final Logger logger);

    /**
     * Create specific test configuration.
     * @param format
     * @return
     * @throws Exception
     */
    protected abstract Configuration.TestConfiguration testConfiguration(final DataFormat format) throws Exception;
}
