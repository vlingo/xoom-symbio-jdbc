// Copyright © 2012-2021 VLINGO LABS. All rights reserved.
//
// This Source Code Form is subject to the terms of the
// Mozilla Public License, v. 2.0. If a copy of the MPL
// was not distributed with this file, You can obtain
// one at https://mozilla.org/MPL/2.0/.

package io.vlingo.xoom.symbio.store.object.jdbc.jdbi;

import java.util.function.BiFunction;

import org.jdbi.v3.core.statement.Update;

/**
 * A Jdbi-based {@code PersistentObjectMapper::persistMapper}.
 */
public class JdbiPersistMapper {
  /**
   * The default column name of the the column of the integral id; generally auto-generated.
   * The type of the column is assumed to be BIGINT (Long).
   */
  public static final String DefaultIdColumnName = "id";

  public final String idColumnName;
  public final String insertStatement;
  public final String updateStatement;
  public final BiFunction<Update,Object,Update>[] binders;

  /**
   * Answer a new {@code JdbiPersistMapper} with {@code insertStatement}, {@code updateStatement}, and {@code binder}.
   * @param insertStatement the String insert statement
   * @param updateStatement the String update statement
   * @param binders the {@code BiFunction<Update,Object,Update> ...} used to bind parameters
   * @return JdbiPersistMapper
   * @see JdbiPersistMapper#DefaultIdColumnName
   */
  @SafeVarargs
  public static JdbiPersistMapper with(final String insertStatement, final String updateStatement, final BiFunction<Update,Object,Update> ... binders) {
    return new JdbiPersistMapper(insertStatement, updateStatement, binders);
  }

  /**
   * Answer a new {@code JdbiPersistMapper} with {@code insertStatement}, {@code updateStatement}, and {@code binder}.
   * @param idColumnName the String name of the column of the integral id; generally auto-generated
   * @param insertStatement the String insert statement
   * @param updateStatement the String update statement
   * @param binders the {@code BiFunction<Update,Object,Update> ...} used to bind parameters
   * @return JdbiPersistMapper
   * @see JdbiPersistMapper#DefaultIdColumnName
   */
  @SafeVarargs
  public static JdbiPersistMapper with(final String idColumnName, final String insertStatement, final String updateStatement, final BiFunction<Update,Object,Update> ... binders) {
    return new JdbiPersistMapper(idColumnName, insertStatement, updateStatement, binders);
  }

  /**
   * Construct my state with {@code insertStatement}, {@code updateStatement}, and {@code binder}.
   * @param insertStatement the String insert statement
   * @param updateStatement the String update statement
   * @param binders the {@code BiFunction<Update,Object,Update> ...} used to bind parameters
   * @see JdbiPersistMapper#DefaultIdColumnName
   */
  @SafeVarargs
  public JdbiPersistMapper(final String insertStatement, final String updateStatement, final BiFunction<Update,Object,Update> ... binders) {
    this(DefaultIdColumnName, insertStatement, updateStatement, binders);
  }

  /**
   * Construct my state with {@code insertStatement}, {@code updateStatement}, and {@code binder}.
   * @param idColumnName the String name of the column of the integral id; generally auto-generated
   * @param insertStatement the String insert statement
   * @param updateStatement the String update statement
   * @param binders the {@code BiFunction<Update,Object,Update> ...} used to bind parameters
   * @see JdbiPersistMapper#DefaultIdColumnName
   */
  @SafeVarargs
  public JdbiPersistMapper(final String idColumnName, final String insertStatement, final String updateStatement, final BiFunction<Update,Object,Update> ... binders) {
    this.idColumnName = idColumnName;
    this.insertStatement = insertStatement;
    this.updateStatement = updateStatement;
    this.binders = binders;
  }
}
