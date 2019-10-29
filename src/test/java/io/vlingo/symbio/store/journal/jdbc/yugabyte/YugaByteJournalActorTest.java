// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.journal.jdbc.yugabyte;

import io.vlingo.symbio.store.DataFormat;
import io.vlingo.symbio.store.common.jdbc.Configuration;
import io.vlingo.symbio.store.common.jdbc.yugabyte.YugaByteConfigurationProvider;
import io.vlingo.symbio.store.journal.jdbc.JDBCJournalActorTest;
import org.junit.Ignore;

@Ignore
public class YugaByteJournalActorTest extends JDBCJournalActorTest {
    @Override
    protected Configuration.TestConfiguration testConfiguration(DataFormat format) throws Exception {
        return YugaByteConfigurationProvider.testConfiguration(format);
    }
}
