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

package com.moz.fiji.rest.util;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.schema.HBaseEntityId;
import com.moz.fiji.schema.Fiji;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiURI;

/**
 * A cache object containing all Fiji, FijiTable, and FijiTableReader objects for a Fiji
 * instance. Handles the creation and lifecycle of instances.
 */
public class FijiInstanceCache {

  private static final Logger LOG = LoggerFactory.getLogger(FijiInstanceCache.class);

  private static final long TEN_MINUTES = 10 * 60 * 1000;

  /** Determines whether new values can be loaded into the contained caches. */
  private volatile boolean mIsOpen = true;

  private final Fiji mFiji;

  private final LoadingCache<String, FijiTable> mTables =
      CacheBuilder.newBuilder()
          // Expire table if it has not been used in 10 minutes
          // TODO (REST-133): Make this value configurable
          .expireAfterAccess(10, TimeUnit.MINUTES)
          .removalListener(
              new RemovalListener<String, FijiTable>() {
                @Override
                public void onRemoval(RemovalNotification<String, FijiTable> notification) {
                  try {
                    notification.getValue().release(); // strong cache; should not be null
                  } catch (IOException e) {
                    LOG.warn("Unable to release FijiTable {} with name {}.",
                        notification.getValue(), notification.getValue());
                  }
                }
              }
          )
          .build(
              new CacheLoader<String, FijiTable>() {
                @Override
                public FijiTable load(String table) throws IOException {
                  Preconditions.checkState(mIsOpen, "Cannot open FijiTable in closed cache.");
                  return mFiji.openTable(table);
                }
              }
          );

  private final LoadingCache<String, FijiTableReader> mReaders =
      CacheBuilder.newBuilder()
          // Expire reader if it has not been used in 10 minutes
          // TODO (REST-133): Make this value configurable
          .expireAfterAccess(10, TimeUnit.MINUTES)
          .removalListener(
              new RemovalListener<String, FijiTableReader>() {
                @Override
                public void onRemoval(
                    RemovalNotification<String,
                    FijiTableReader> notification
                ) {
                  try {
                    notification.getValue().close(); // strong cache; should not be null
                  } catch (IOException e) {
                    LOG.warn("Unable to close FijiTableReader {} on table {}.",
                        notification.getValue(), notification.getValue());
                  }
                }
              }
          )
          .build(
              new CacheLoader<String, FijiTableReader>() {
                @Override
                public FijiTableReader load(String table) throws IOException {
                  try {
                    Preconditions.checkState(mIsOpen,
                        "Cannot open FijiTableReader in closed cache.");
                    return mTables.get(table).openTableReader();
                  } catch (ExecutionException e) {
                    // Unwrap (if possible) and rethrow. Will be caught by #getFijiTableReader.
                    final Throwable cause = e.getCause();
                    if (cause instanceof IOException) {
                      throw (IOException) cause;
                    } else {
                      throw new IOException(cause);
                    }
                  }
                }
              }
          );

  /**
   *
   * Create a new FijiInstanceCache which caches the instance at the provided URI.
   *
   * @param uri of instance to cache access to.
   * @throws IOException if error while opening fiji.
   */
  public FijiInstanceCache(FijiURI uri) throws IOException {
    mFiji = Fiji.Factory.open(uri);
  }

  /**
   * Returns the Fiji instance held by this cache.  This Fiji instance should *NOT* be released.
   *
   * @return a Fiji instance.
   */
  public Fiji getFiji() {
    return mFiji;
  }

  /**
   * Returns the FijiTable instance for the table name held by this cache.  This FijiTable instance
   * should *NOT* be released.
   *
   * @param table name.
   * @return the FijiTable instance
   * @throws java.util.concurrent.ExecutionException if the table cannot be created.
   */
  public FijiTable getFijiTable(String table) throws ExecutionException {
    return mTables.get(table);
  }

  /**
   * Returns the FijiTableReader instance for the table held by this cache.  This
   * FijiTableReader instance should *NOT* be closed.
   *
   * @param table name.
   * @return a FijiTableReader for the table.
   * @throws ExecutionException if a FijiTableReader cannot be created for the table.
   */
  public FijiTableReader getFijiTableReader(String table) throws ExecutionException {
    return mReaders.get(table);
  }

  /**
   * Invalidates cached FijiTable and FijiTableReader instances for a table.
   *
   * @param table name to be invalidated.
   */
  public void invalidateTable(String table) {
    mTables.invalidate(table);
    mReaders.invalidate(table);
  }

  /**
   * Stop creating resources to cache, and cleanup any existing resources.
   *
   * @throws IOException if error while closing instance.
   */
  public void stop() throws IOException {
    mIsOpen = false; // Stop caches from loading more entries
    mReaders.invalidateAll();
    mReaders.cleanUp();
    mTables.invalidateAll();
    mTables.cleanUp();
    mFiji.release();
  }

  /**
   * Checks the health of this FijiInstanceCache, and all the stateful objects it contains.
   *
   * @return a list health issues.  Will be empty if the cache is healthy.
   */
  public List<String> checkHealth() {
    ImmutableList.Builder<String> issues = ImmutableList.builder();
    if (mIsOpen) {

      // Check that the Fiji instance is healthy
      try {
        mFiji.getMetaTable();
      } catch (IllegalStateException e) {
        issues.add(String.format("Fiji instance %s is in illegal state.", mFiji));
      } catch (IOException e) {
        issues.add(String.format("Fiji instance %s cannot open meta table.", mFiji));
      }

      // Check that the FijiTable instances are healthy
      for (FijiTable table : mTables.asMap().values()) {
        try {
          table.getWriterFactory();
        } catch (IllegalStateException e) {
          issues.add(String.format("FijiTable instance %s is in illegal state.", table));
        } catch (IOException e) {
          issues.add(String.format("FijiTable instance %s cannot open reader factory.", table));
        }
      }

      // Check that the FijiTableReader instances are healthy
      for (FijiTableReader reader : mReaders.asMap().values()) {
        try {
          reader.get(HBaseEntityId.fromHBaseRowKey(new byte[0]), FijiDataRequest.empty());
        } catch (IllegalStateException e) {
          issues.add(String.format("FijiTableReader instance %s is in illegal state.",
              reader));
        } catch (IOException e) {
          issues.add(String.format("FijiTableReader instance %s cannot get data.",
              reader));
        }
      }
    } else {
      issues.add(String.format("FijiInstanceCache for fiji instance %s is not open.",
          mFiji.getURI().getInstance()));
    }
    return issues.build();
  }
}
