// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import io.vlingo.symbio.store.common.jdbc.Configuration;
import org.jdbi.v3.core.statement.Update;

import java.text.MessageFormat;
import java.util.function.BiFunction;

public class JdbiOnMySQL extends JdbiOnDatabase {
    /**
     * Answer a new {@code JdbiOnMySQL} using the {@code Configuration}.
     * @param configuration the Configuration which include the Connection
     * @return JdbiOnMySQL
     */
    public static JdbiOnMySQL openUsing(final Configuration configuration) {
        return new JdbiOnMySQL(configuration);
    }

    /*
     * @see io.vlingo.symbio.store.object.jdbc.jdbi.JdbiOnDatabase#currentEntryOffsetMapper(java.lang.String[])
     */
    @Override
    public JdbiPersistMapper currentEntryOffsetMapper(final String[] placeholders) {
        return JdbiPersistMapper.with(
                MessageFormat.format(
                        "INSERT INTO TBL_VLINGO_OBJECTSTORE_ENTRYREADER_OFFSETS(O_READER_NAME, O_READER_OFFSET) VALUES({0}, {1}) " +
                                "ON CONFLICT (O_READER_NAME) DO UPDATE SET O_READER_OFFSET=?",
                        placeholders[0],
                        placeholders[1]),
                null,
                (BiFunction<Update,Object,Update>[]) null);
    }

    private JdbiOnMySQL(final Configuration configuration) {
        super(configuration);
    }
}
