/*
 * This module, both source code and documentation,
 * is in the Public Domain, and comes with NO WARRANTY.
 */
package com.github.blagerweij.sessionlock;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Locale;
import liquibase.database.Database;
import liquibase.database.core.MSSQLDatabase;
import liquibase.exception.LockException;
import liquibase.lockservice.DatabaseChangeLogLock;

/**
 * Employs MS SQL Server session locks.
 *
 * <blockquote>
 *
 * <p>A lock obtained with <code>sp_getapplock</code> is released explicitly by executing <code>
 * sp_releaseapplock</code> or implicitly when your session terminates (either normally or abnormally).
 * Locks obtained with <code>sp_getapplock</code> (in Session mode) are not released when transactions commit or roll
 * back.
 *
 * </blockquote>
 *
 * @see "<a href='https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-getapplock-transact-sql?view=sql-server-ver15'>Locking
 *     Functions</a> (SQL Server Manual)"
 */
public class SQLServerLockService extends SessionLockService {

  static final String SQL_GET_LOCK = "DECLARE @i int; EXEC @i = sp_getapplock @Resource= ?, @LockMode = 'Exclusive', @LockOwner='Session', @LockTimeout=?; SELECT @i;";
  static final String SQL_RELEASE_LOCK = "DECLARE @i INT; EXEC @i = sp_releaseapplock @Resource= ?, @LockOwner= 'Session'; SELECT @i;";
  static final String SQL_LOCK_INFO =
      "SELECT s.session_id, l.resource_description, s.login_time, s.host_name "
      + "FROM sys.dm_tran_locks l INNER JOIN sys.dm_exec_sessions s ON (s.session_id = l.request_session_id) "
      + "WHERE request_owner_type = 'SESSION' "
      + "AND l.resource_description LIKE ?  ";

  @Override
  public boolean supports(Database database) {
    return (database instanceof MSSQLDatabase) && !isSessionLockingDisabled();
  }

  private String getChangeLogLockName() {
    return (database.getDefaultSchemaName() + "." + database.getDatabaseChangeLogLockTableName())
        .toUpperCase(Locale.ROOT);
  }

  private static Integer getIntegerResult(PreparedStatement stmt) throws SQLException {
    try (ResultSet rs = stmt.executeQuery()) {
      rs.next();
      Number locked = (Number) rs.getObject(1);
      return (locked == null) ? null : locked.intValue();
    }
  }

  /**
   * Acquire lock with sp_getapplock()
   */
  @Override
  protected boolean acquireLock(Connection con) throws SQLException, LockException {
    try (PreparedStatement stmt = con.prepareStatement(SQL_GET_LOCK)) {
      stmt.setString(1, getChangeLogLockName());
      final int timeoutSeconds = 5;
      stmt.setInt(2, timeoutSeconds*1000);

      Integer locked = getIntegerResult(stmt);
      if (locked == null) {
        throw new LockException("sp_getapplock() returned NULL");
      } else if (locked < -3) {
        throw new LockException("sp_getapplock() returned " + locked);
      } else if (locked < 0) {
        return false;
      }
      return true;
    }
  }

  /**
   * Acquire lock with sp_releaseapplock()
   */
  @Override
  protected void releaseLock(Connection con) throws SQLException, LockException {
    try (PreparedStatement stmt = con.prepareStatement(SQL_RELEASE_LOCK)) {
      stmt.setString(1, getChangeLogLockName());

      Integer unlocked = getIntegerResult(stmt);
      if (!Integer.valueOf(0).equals(unlocked)) {
        throw new LockException(
            "sp_releaseapplock() returned " + String.valueOf(unlocked).toUpperCase(Locale.ROOT));
      }
    }
  }

  /**
   * Obtains information about the database changelog lock.
   */
  @Override
  protected DatabaseChangeLogLock usedLock(Connection con) throws SQLException, LockException {
    try (PreparedStatement stmt = con.prepareStatement(SQL_LOCK_INFO)) {
      stmt.setString(1, getChangeLogLockName());

      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next() || rs.getObject("resource_description") == null) {
          return null;
        }

        Timestamp timestamp = rs.getTimestamp("login_time");
        return new DatabaseChangeLogLock(1, timestamp, lockedBy(rs));
      }
    }
  }

  private static String lockedBy(ResultSet rs) throws SQLException {
    String host = rs.getString("host_name");
    if (host == null || host.trim().isEmpty()) {
      return "session_id#" + rs.getInt("sessionId");
    }
    return host;
  }
}
