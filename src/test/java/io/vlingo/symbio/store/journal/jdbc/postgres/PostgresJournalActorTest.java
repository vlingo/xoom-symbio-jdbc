package io.vlingo.symbio.store.journal.jdbc.postgres;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.postgres.PostgresConfigurationProvider;
import io.vlingo.symbio.store.journal.jdbc.JDBCJournalActorTest;

public class PostgresJournalActorTest extends JDBCJournalActorTest {

    @Override
    protected Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
        return PostgresConfigurationProvider.testConfiguration(format);
    }
}
