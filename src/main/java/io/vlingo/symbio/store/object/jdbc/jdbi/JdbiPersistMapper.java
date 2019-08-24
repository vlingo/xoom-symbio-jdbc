// Copyright © 2012-2018 Vaughn Vernon. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.symbio.store.object.jdbc.jdbi;

import java.util.function.BiFunction;

import org.jdbi.v3.core.statement.Update;

/**
 * A Jdbi-based {@code PersistentObjectMapper::persistMapper}.
 */
public class JdbiPersistMapper {
  public final String insertStatement;
  public final String updateStatement;
  public final BiFunction<Update,Object,Update>[] binders;

  /**
   * Answer a new {@code JdbiPersistMapper} with {@code insertStatement}, {@code updateStatement}, and {@code binder}.
   * @param insertStatement the String insert statement
   * @param updateStatement the String update statement
   * @param binders the {@code BiFunction<Update,Object,Update> ...} used to bind parameters
   * @return JdbiPersistMapper
   */
  @SafeVarargs
  public static JdbiPersistMapper with(final String insertStatement, final String updateStatement, final BiFunction<Update,Object,Update> ... binders) {
    return new JdbiPersistMapper(insertStatement, updateStatement, binders);
  }

  /**
   * Construct my state with {@code insertStatement}, {@code updateStatement}, and {@code binder}.
   * @param insertStatement the String insert statement
   * @param updateStatement the String update statement
   * @param binders the {@code BiFunction<Update,Object,Update> ...} used to bind parameters
   */
  @SafeVarargs
  public JdbiPersistMapper(final String insertStatement, final String updateStatement, final BiFunction<Update,Object,Update> ... binders) {
    this.insertStatement = insertStatement;
    this.updateStatement = updateStatement;
    this.binders = binders;
  }
}
