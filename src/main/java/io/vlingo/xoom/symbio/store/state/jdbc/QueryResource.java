package io.vlingo.xoom.symbio.store.state.jdbc;

import io.vlingo.xoom.symbio.store.common.jdbc.ConnectionProvider;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.BiFunction;

/**
 * {@link ResultSet} resource for a specific SQL query. This class controls (e.g. commit/rollback/close) in addition the lifecycle of subsequent {@link Connection}.
 */
public class QueryResource {
  private final String query;
  private final ConnectionProvider connectionProvider;
  private final BiFunction<Connection, String, PreparedStatement> statementFactory;

  private Connection connection = null;
  private PreparedStatement preparedStatement = null;

  QueryResource(String query, ConnectionProvider connectionProvider, BiFunction<Connection, String, PreparedStatement> statementFactory) {
    this.query = query;
    this.connectionProvider = connectionProvider;
    this.statementFactory = statementFactory;
  }

  boolean isClosed() {
    return connection == null;
  }

  public ResultSet execute() {
    if (!isClosed()) {
      throw new IllegalStateException("Failed to open an already opened QueryResource for SQL query: " + query);
    }

    try {
      this.connection = connectionProvider.newConnection();
      this.preparedStatement = statementFactory.apply(connection, query);
      return this.preparedStatement.executeQuery();
    } catch (Exception e) {
      throw new RuntimeException("Failed to open QueryResource for SQL query: " + query, e);
    }
  }

  public void close() {
    if (isClosed()) {
      throw new IllegalStateException("Failed to close an already closed QueryResource for SQL query: " + query);
    }

    PreparedStatement tempStatement = this.preparedStatement;
    Connection tempConnection = this.connection;

    this.preparedStatement = null;
    this.connection = null;

    try {
      tempStatement.close();
      tempConnection.commit();
      tempConnection.close(); // connection gets back to connection pool
    } catch (Exception e) {
      throw new RuntimeException("Failed to close QueryResource because: " + e.getMessage(), e);
    }
  }

  public void fail() {
    if (isClosed()) {
      throw new IllegalStateException("Failed to rollback an already closed QueryResource for SQL query: " + query);
    }

    PreparedStatement tempStatement = this.preparedStatement;
    Connection tempConnection = this.connection;

    this.preparedStatement = null;
    this.connection = null;

    try {
      tempStatement.close();
      tempConnection.rollback();
      tempConnection.close(); // connection gets back to connection pool
    } catch (SQLException e) {
      throw new RuntimeException("Failed to rollback QueryResource because: " + e.getMessage(), e);
    }
  }
}
