// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.
package io.vlingo.xoom.symbio.store.object.jdbc.jpa;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Before;

import io.vlingo.xoom.actors.Logger;
import io.vlingo.xoom.actors.World;
import io.vlingo.xoom.actors.testkit.AccessSafely;
import io.vlingo.xoom.common.Outcome;
import io.vlingo.xoom.symbio.Entry;
import io.vlingo.xoom.symbio.EntryAdapterProvider;
import io.vlingo.xoom.symbio.State;
import io.vlingo.xoom.symbio.StateAdapterProvider;
import io.vlingo.xoom.symbio.store.Result;
import io.vlingo.xoom.symbio.store.StorageException;
import io.vlingo.xoom.symbio.store.common.MockDispatcher;
import io.vlingo.xoom.symbio.store.common.jdbc.Configuration;
import io.vlingo.xoom.symbio.store.common.jdbc.ConnectionProvider;
import io.vlingo.xoom.symbio.store.object.ObjectStoreReader.QueryMultiResults;
import io.vlingo.xoom.symbio.store.object.ObjectStoreReader.QueryResultInterest;
import io.vlingo.xoom.symbio.store.object.ObjectStoreReader.QuerySingleResult;
import io.vlingo.xoom.symbio.store.object.ObjectStoreWriter.PersistResultInterest;
import io.vlingo.xoom.symbio.store.object.jdbc.JDBCObjectStoreEntryJournalQueries;
import io.vlingo.xoom.symbio.store.object.jdbc.jpa.PersonEntryAdapters.PersonAddedAdapter;
import io.vlingo.xoom.symbio.store.object.jdbc.jpa.PersonEntryAdapters.PersonRenamedAdapter;
import io.vlingo.xoom.symbio.store.object.jdbc.jpa.PersonEvents.PersonAdded;
import io.vlingo.xoom.symbio.store.object.jdbc.jpa.PersonEvents.PersonRenamed;

public abstract class JPAObjectStoreTest {
    protected Configuration adminConfiguration;
    protected ConnectionProvider connectionProvider;
    protected JPAObjectStoreDelegate delegate;
    protected MockDispatcher<Entry<String>, State<String>> dispatcher;
    protected JPAObjectStore objectStore;
    protected JDBCObjectStoreEntryJournalQueries queries;
    protected String testDatabaseName;
    protected World world;

    private static final AtomicInteger databaseNamePostfix = new AtomicInteger(Short.MAX_VALUE);

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        world = World.startWithDefaults("jpa-test");
        EntryAdapterProvider.instance(world).registerAdapter(PersonAdded.class, new PersonAddedAdapter());
        EntryAdapterProvider.instance(world).registerAdapter(PersonRenamed.class, new PersonRenamedAdapter());
        final StateAdapterProvider stateAdapterProvider = StateAdapterProvider.instance(world);
        dispatcher = new MockDispatcher<>();
        adminConfiguration = createAdminConfiguration();

        testDatabaseName = testDatabaseName();
        dropTestDatabase();
        createTestDatabase();
        final Map<String,Object> properties = testDatabaseProperties(testDatabaseName);

        // delegate = new JPAObjectStoreDelegate(JPAObjectStoreDelegate.JPA_HSQLDB_PERSISTENCE_UNIT, "TEST", stateAdapterProvider, world.defaultLogger());
        delegate = createDelegate(properties, "TEST", stateAdapterProvider, world.defaultLogger());

        // connectionProvider = new ConnectionProvider("org.hsqldb.jdbc.JDBCDriver", "jdbc:hsqldb:mem", "test", "SA", "", false);
        connectionProvider = createConnectionProvider();

        // queries = new HSQLDBObjectStoreEntryJournalQueries(connectionProvider.connection());
        queries = createQueries();

        objectStore = world.actorFor(JPAObjectStore.class, JPAObjectStoreActor.class, delegate, connectionProvider, dispatcher);
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
        if (objectStore != null) {
            objectStore.close();
        }
        world.terminate();
        dropTestDatabase();
    }

    protected String testDatabaseName() {
        final int postfix = databaseNamePostfix.incrementAndGet();
        return "xoom_test_" + Math.abs(postfix);
    }

    protected Map<String,Object> testDatabaseProperties(final String databaseNamePostfix) {
        final Map<String, Object> properties = new HashMap<>();
        Map<String, Object> specificProperties = getDatabaseSpecificProperties(databaseNamePostfix);

        properties.putAll(specificProperties);
        properties.put("javax.persistence.LockModeType", "OPTIMISTIC_FORCE_INCREMENT");
        properties.put("javax.persistence.schema-generation.database.action", "drop-and-create");
        properties.put("javax.persistence.schema-generation.create-database-schemas", "drop-and-create");
        properties.put("javax.persistence.schema-generation.scripts.action", "drop-and-create");
        properties.put("javax.persistence.schema-generation.scripts.create-target", "./target/createDDL.jdbc");
        properties.put("javax.persistence.schema-generation.scripts.drop-target", "./target/dropDDL.jdbc");
        properties.put("javax.persistence.schema-generation.create-source", "metadata");
        properties.put("javax.persistence.schema-generation.drop-source", "metadata");
        properties.put("javax.persistence.schema-generation.create-script-source", "./target/createDDL.jdbc");
        properties.put("javax.persistence.schema-generation.drop-script-source", "./target/dropDDL.jdbc");

        return properties;
    }

    public static class TestQueryResultInterest implements QueryResultInterest {
        public AtomicReference<QueryMultiResults> multiResults = new AtomicReference<>();
        public AtomicReference<QuerySingleResult> singleResult = new AtomicReference<>();
        private AccessSafely access = AccessSafely.afterCompleting(1);

        @Override
        public void queryAllResultedIn(final Outcome<StorageException, Result> outcome, final QueryMultiResults results, final Object object) {
            access.writeUsing( "addAll", results );
        }

        @Override
        public void queryObjectResultedIn(final Outcome<StorageException, Result> outcome, final QuerySingleResult result, final Object object) {
            access.writeUsing( "add", result );
        }

        public AccessSafely afterCompleting( final int times ) {
            access =
                    AccessSafely
                            .afterCompleting( times )
                            .writingWith("add", (value)-> singleResult.set((QuerySingleResult)value))
                            .writingWith("addAll", (values) -> multiResults.set((QueryMultiResults)values))
                            .readingWith("singleResult", () -> singleResult.get())
                            .readingWith("singleResultValue", () -> singleResult.get().stateObject)
                            .readingWith("multiResults", () -> multiResults.get())
                            .readingWith("multiResultsValue", () -> multiResults.get().stateObjects)
                            .readingWith("multiResultsSize", () -> multiResults.get().stateObjects().size())
                            .readingWith("multiResultsValue", (index) -> {
                                Person person = null;
                                if ( multiResults.get().stateObjects.size() > (int)index )
                                {
                                    person = (Person)multiResults.get().stateObjects.toArray()[(int)index];
                                }
                                return person;
                            })
                            .readingWith("multiResultsValueAsArray", () -> multiResults.get().stateObjects.toArray());
            return access;
        }
    }

    public static class TestPersistResultInterest implements PersistResultInterest {
        public AtomicReference<Outcome<StorageException, Result>> outcome = new AtomicReference<>();
        public AtomicInteger count = new AtomicInteger(0);
        private AccessSafely access = AccessSafely.afterCompleting(1);

        @Override
        public void persistResultedIn(Outcome<StorageException, Result> outcome, Object persistentObject, int possible, int actual, Object object) {
            access.writeUsing("set", outcome);
        }

        @SuppressWarnings("unchecked")
        public AccessSafely afterCompleting( final int times ) {
            access =
                    AccessSafely
                            .afterCompleting(times)
                            .writingWith("set", (value) -> {
                                outcome.set((Outcome<StorageException,Result>)value);
                                count.getAndAdd(1);
                            })
                            .readingWith("result", () -> outcome.get().andThen(success -> success).get())
                            .readingWith("outcome", () -> outcome.get())
                            .readingWith("count", () -> count.get());
            return access;
        }
    }

    protected abstract Configuration createAdminConfiguration() throws Exception;

    protected abstract JPAObjectStoreDelegate createDelegate(final Map<String,Object> properties, final String originatorId,
                                                             final StateAdapterProvider stateAdapterProvider, final Logger logger);

    protected abstract ConnectionProvider createConnectionProvider();

    protected abstract JDBCObjectStoreEntryJournalQueries createQueries();

    protected abstract void createTestDatabase() throws Exception;

    protected abstract void dropTestDatabase() throws Exception;

    protected abstract Map<String, Object> getDatabaseSpecificProperties(String databaseNamePostfix);
}
