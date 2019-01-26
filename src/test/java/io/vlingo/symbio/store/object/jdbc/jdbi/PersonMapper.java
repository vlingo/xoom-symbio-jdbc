// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;

import io.vlingo.symbio.store.object.jdbc.Person;

public class PersonMapper implements RowMapper<Person> {
  @Override
  public Person map(final ResultSet rs, final StatementContext ctx) throws SQLException {
    return new Person(rs.getString("name"), rs.getInt("age"), rs.getLong("id"));
  }
}
