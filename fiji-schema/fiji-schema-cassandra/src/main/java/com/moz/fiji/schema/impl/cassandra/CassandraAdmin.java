/**
 * (c) Copyright 2014 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.moz.fiji.schema.impl.cassandra;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.avro.RowKeyFormat2;
import com.moz.fiji.schema.cassandra.CassandraFijiURI;
import com.moz.fiji.schema.cassandra.CassandraTableName;
import com.moz.fiji.schema.cassandra.util.SessionCache;

/**
 * Lightweight wrapper to mimic the functionality of HBaseAdmin (and provide other functionality).
 *
 * This class exists mostly so that we are not passing around instances of
 * {@link com.datastax.driver.core.Session} everywhere.
 */
@ApiAudience.Private
@ThreadSafe
public final class CassandraAdmin implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(CassandraAdmin.class);

  /** Current C* session for the Fiji instance.. */
  private final Session mSession;

  /** URI for this instance. **/
  private final CassandraFijiURI mFijiURI;

  /** Keep a cache of all of the prepared CQL statements. */
  private final CassandraStatementCache mStatementCache;

  private final LoadingCache<RowKeyFormat2, CQLStatementCache> mStatementCaches =
      CacheBuilder
          .newBuilder()
          .expireAfterAccess(15, TimeUnit.MINUTES) // Avoid holding on to one-off tables
          .build(
              new CacheLoader<RowKeyFormat2, CQLStatementCache>() {
                /** {@inheritDoc} */
                @Override
                public CQLStatementCache load(final RowKeyFormat2 rowKeyFormat) {
                  return new CQLStatementCache(getSession(), rowKeyFormat);
                }
              });

  /**
   * CassandraAdmin private constructor.
   *
   * @param fijiURI The Cassandra Fiji URI of the instance.
   * @param session A connection to the Cassandra cluster.
   * @param statementCache The CQL statement cache.
   */
  private CassandraAdmin(
      final CassandraFijiURI fijiURI,
      final Session session,
      final CassandraStatementCache statementCache
  ) {
    mFijiURI = Preconditions.checkNotNull(fijiURI);
    mSession = Preconditions.checkNotNull(session);
    mStatementCache = Preconditions.checkNotNull(statementCache);
    createKeyspaceIfMissingForURI(fijiURI);
  }

  /**
   * Creates a CassandraAdmin object for a given Fiji instance.
   *
   * @param fijiURI URI for the Fiji cluster.
   * @return A new CassandraAdmin.
   */
  public static CassandraAdmin create(final CassandraFijiURI fijiURI) {
    final Session session = SessionCache.getSession(fijiURI);
    final CassandraStatementCache statementCache = new CassandraStatementCache(session);
    return new CassandraAdmin(fijiURI, session, statementCache);
  }

  /**
   * Getter for open Session.
   *
   * @return The Session.
   */
  protected Session getSession() {
    return mSession;
  }

  /**
   * Given a URI, create a keyspace for the Fiji instance if none yet exists.
   *
   * @param fijiURI The URI.
   */
  private void createKeyspaceIfMissingForURI(FijiURI fijiURI) {
    String keyspace = CassandraTableName.getKeyspace(fijiURI);

    // Do a check first for the existence of the appropriate keyspace.  If it exists already, then
    // don't try to create it.
    if (!keyspaceExists(keyspace)) {
      LOG.debug(String.format("Creating keyspace %s for Fiji instance %s.", keyspace, fijiURI));

      // TODO: Check whether keyspace is > 48 characters long and if so provide Fiji error to user.
      String queryText = "CREATE KEYSPACE IF NOT EXISTS " + keyspace
          + " WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 3}";
      getSession().execute(queryText);
    }
    // Check that the keyspace actually exists!
    assert(keyspaceExists(keyspace));
  }

  /**
   * Check whether a keyspace exists.
   *
   * @param keyspace The keyspace name (can include quotes - this method strips them).
   * @return Whether the keyspace exists.
   */
  private boolean keyspaceExists(String keyspace) {
    return (null != mSession.getCluster().getMetadata().getKeyspace(keyspace));
  }

  /**
   * Create a table in the given keyspace.
   *
   * This wrapper exists (rather than having various classes create tables themselves) so that we
   * can add lots of extra boilerplate checks in here.
   *
   * @param tableName The name of the table to create.
   * @param createTableStatement A string with the table layout.
   */
  public void createTable(CassandraTableName tableName, String createTableStatement) {
    // TODO: Keep track of all tables associated with this session
    LOG.debug("Creating table {} with statement {}.", tableName, createTableStatement);
    getSession().execute(createTableStatement);

    // Check that the table actually exists
    assert(tableExists(tableName));
  }

  /**
   * Check whether the keyspace for this Fiji instance is empty.
   *
   * @return whether the keyspace is empty.
   */
  public boolean keyspaceIsEmpty() {
    Preconditions.checkNotNull(getSession());
    Preconditions.checkNotNull(getSession().getCluster());
    Preconditions.checkNotNull(getSession().getCluster().getMetadata());
    String keyspace = CassandraTableName.getKeyspace(mFijiURI);
    Preconditions.checkNotNull(getSession().getCluster().getMetadata().getKeyspace(keyspace));
    Collection<TableMetadata> tables =
        getSession().getCluster().getMetadata().getKeyspace(keyspace).getTables();
    return tables.isEmpty();
  }

  /**
   * Delete the keyspace for this Fiji instance.
   */
  public void deleteKeyspace() {
    // TODO: Track whether keyspace exists and assert appropriate keyspace state in all methods.
    String keyspace = CassandraTableName.getKeyspace(mFijiURI);
    String queryString = "DROP KEYSPACE " + keyspace;
    getSession().execute(queryString);
    assert (!keyspaceExists(keyspace));
  }

  /**
   * Check whether a given Cassandra table exists.
   *
   * Useful for double checking that `CREATE TABLE` statements have succeeded.
   *
   * @param tableName of the Cassandra table to check for.
   * @return Whether the table exists.
   */
  public boolean tableExists(CassandraTableName tableName) {
    Preconditions.checkNotNull(getSession());
    Metadata metadata = getSession().getCluster().getMetadata();

    String keyspace = CassandraTableName.getKeyspace(mFijiURI);

    if (null == metadata.getKeyspace(keyspace)) {
      assert(!keyspaceExists(CassandraTableName.getKeyspace(mFijiURI)));
      return false;
    }

    return metadata.getKeyspace(keyspace).getTable(tableName.getTable()) != null;
  }

  /** {@inheritDoc} */
  @Override
  public void close() {
    LOG.debug("Closing connection to Cassandra: '{}'. Cluster: '{}'.",
        mFijiURI, mSession.getCluster().getClusterName());
    getSession().close();
  }

  /**
   * Retrieve the statement cache for a given Cassandra table.
   *
   * @param rowKeyFormat The rowKeyFormat of the table.
   * @return The statement cache for the table.
   */
  public CQLStatementCache getStatementCache(final RowKeyFormat2 rowKeyFormat) {
    return mStatementCaches.getUnchecked(rowKeyFormat);
  }

  // ----------------------------------------------------------------------------------------------
  // Code to wrap around the Cassandra Session to ensure that all queries are cached.

  /**
   * Execute a statement, using a prepared statement if one already exists for the statement, and
   * creating and caching a prepared statement otherwise.
   *
   * @param statement The statement to execute.
   * @return The results of executing the statement.
   */
  public ResultSet execute(Statement statement) {
    return mSession.execute(statement);
  }

  /**
   * Execute a query, using a prepared statement if one already exists for the query, and creating
   * and caching a prepared statement otherwise.
   *
   * @param query The query to execute.
   * @return The results of executing the query.
   */
  public ResultSet execute(String query) {
    PreparedStatement preparedStatement = mStatementCache.getPreparedStatement(query);
    return mSession.execute(preparedStatement.bind());
  }

  /**
   * Asynchronously execute a statement, using a prepared statement if one already exists for the
   * statement, and creating and caching a prepared statement otherwise.
   *
   * @param statement The statement to execute.
   * @return The results of executing the statement.
   */
  public ResultSetFuture executeAsync(Statement statement) {
    return mSession.executeAsync(statement);
  }

  /**
   * Asynchronously execute a query, using a prepared statement if one already exists for the
   * query, and creating and caching a prepared statement otherwise.
   *
   * @param query The query to execute.
   * @return The results of executing the query.
   */
  public ResultSetFuture executeAsync(String query) {
    PreparedStatement preparedStatement = mStatementCache.getPreparedStatement(query);
    return mSession.executeAsync(preparedStatement.bind());
  }

  /**
   * Get the prepared statement for a query.
   *
   * @param query for which to get the prepared statement.
   * @return a prepared statement for the query.
   */
  public PreparedStatement getPreparedStatement(String query) {
    return mStatementCache.getPreparedStatement(query);
  }
}
