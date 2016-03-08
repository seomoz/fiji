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

package com.moz.fiji.commons;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.NotThreadSafe;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;

/**
 * A keyed cache which keeps track of how many outstanding users of the cached value exist.
 *
 * <p>
 *   When the reference count falls to 0, the cached value (which must implement {@link Closeable}),
 *   is removed from the cache and closed.
 * </p>
 *
 * <p>
 *   Clients of {@code ReferenceCountedCache} *must* have a corresponding {@link #release} call
 *   for every {@link #get} after the cached object will no longer be used. After the
 *   {@link #release} call, the cached object must no longer be used.
 * </p>
 *
 * <p>
 *   The {@code ReferenceCountedCache} may optionally be closed, which will preemptively close all
 *   cached entries regardless of reference count. See the javadoc of {@link #close()} for caveats.
 * </p>
 *
 * <p>
 *   The {@code ReferenceCountedCache} may optionally be created with tracking. Caches with tracking
 *   will automatically register and unregister cached values with the {@link ResourceTracker}
 *   when they are added and removed from the cache. Registration of values happens once for the
 *   lifetime of the object (not every time the object is loaned out).
 * </p>
 *
 * @param <K> key type of cache.
 * @param <V> value type (must implement {@link Closeable}) of cache.
 */
@ApiAudience.Framework
@ApiStability.Experimental
@ThreadSafe
public final class ReferenceCountedCache<K, V extends Closeable> implements Closeable {
  private final ConcurrentMap<K, CacheEntry<V>> mMap = Maps.newConcurrentMap();

  private final Function<? super K, ? extends V> mCacheLoader;
  private final boolean mTracking;

  /**
   * Flag to indicate whether this reference counted cache is still open. Must only be mutated
   * by {@link #close()} to set it to false.
   */
  private volatile boolean mIsOpen = true;

  /**
   * Private default constructor.
   *
   * @param cacheLoader A function to compute cached values on demand. Should never return null.
   * @param tracking Whether the cached values should be registered with the resource tracker.
   */
  private ReferenceCountedCache(
      final Function<? super K, ? extends V> cacheLoader,
      final boolean tracking
  ) {
    mCacheLoader = cacheLoader;
    mTracking = tracking;
  }

  /**
   * Create a {@code ReferenceCountedCache} using the supplied function to create cached values
   * on demand.
   *
   * <p>
   *   The function may be evaluated with the same key multiple times in the case that the key
   *   becomes invalidated due to the reference count falling to 0. A function need not handle the
   *   case of a null key, but it should never return a null value for any key.
   * </p>
   *
   * @param cacheLoader A function to compute cached values on demand. Should never return null.
   * @return A new {@link ReferenceCountedCache}.
   * @param <K> The key type of the cache.
   * @param <V> The value type (must implement {@link Closeable}) of the cache.
   */
  public static <K, V extends Closeable> ReferenceCountedCache<K, V> create(
      final Function<? super K, ? extends V> cacheLoader
  ) {
    return new ReferenceCountedCache<>(cacheLoader, false);
  }

  /**
   * Create a {@code ReferenceCountedCache} using the supplied function to create cached values
   * on demand with resource tracking of cached values.
   *
   * <p>
   *   The function may be evaluated with the same key multiple times in the case that the key
   *   becomes invalidated due to the reference count falling to 0. A function need not handle the
   *   case of a null key, but it should never return a null value for any key.
   * </p>
   *
   * <p>
   *   The cached values will be registered with the {@link ResourceTracker}, and automatically
   *   unregistered when their reference count falls to 0 and they are removed from the cache. The
   *   values will be registered once upon creation, not every time the value is borrowed. If the
   *   value objects already perform resource tracking, then prefer using {@link #create} to avoid
   *   double registration.
   * </p>
   *
   * @param cacheLoader A function to compute cached values on demand. Should never return null.
   * @return A new {@link ReferenceCountedCache}.
   * @param <K> The key type of the cache.
   * @param <V> The value type (must implement {@link Closeable}) of the cache.
   */
  public static <K, V extends Closeable> ReferenceCountedCache<K, V> createWithResourceTracking(
      final Function<? super K, ? extends V> cacheLoader
  ) {
    return new ReferenceCountedCache<>(cacheLoader, true);
  }

  /*
    Notes on thread safety, etc:

    This cache uses a lock-per-entry scheme to reduce lock contention across threads accessing
    different keys. Any access to a CacheEntry must be synchronized on that instance.
   */

  /**
   * Returns the value associated with {@code key} in this cache, first loading that value if
   * necessary. No observable state associated with this cache is modified until loading completes.
   *
   * @param key for which to retrieve cached value.
   * @return cached value associated with the key.
   */
  public V get(final K key) {
    Preconditions.checkState(mIsOpen, "ReferenceCountedCache is closed.");
    Preconditions.checkNotNull(key);
    while (true) {
      final CacheEntry<V> entry = mMap.get(key);
      if (entry != null) {
        // There is a cached entry.
        synchronized (entry) {
          // We need to check that the cached entry is still valid, because we didn't hold the
          // entry's lock when we retrieved it.
          if (entry.getCount() > 0) {
            // The entry is still valid.
            entry.incrementAndGetCount();
            return entry.getValue();
          }
          // Someone else cleaned up the retrieved entry; try again.
        }
      } else {
        // No cached entry; attempt to make a new one.
        final CacheEntry<V> newEntry = new CacheEntry<>();
        synchronized (newEntry) {
          // Synchronize on `newEntry` so that no one else can use it in the time between
          // we insert it in the cache and we set the value.
          if (mMap.putIfAbsent(key, newEntry) == null) {
            // We successfully made a new entry and cached it
            V value = mCacheLoader.apply(key);
            newEntry.setValue(value);
            newEntry.incrementAndGetCount();
            if (mTracking) {
              ResourceTracker.get().registerResource(value);
            }
            return value;
          }
          // Someone else created it first; try again.
        }
      }
    }
  }

  /**
   * Indicate to the cache that the client is done using the cached value for the given key. Clients
   * of {@link ReferenceCountedCache} must call this after finishing using the value retrieved from
   * the cache.  The retrieved is no longer guaraunteed to be valid after it is released back to the
   * {@link ReferenceCountedCache}
   *
   * @param key to cache entry no longer being used by the client.
   * @throws java.io.IOException if closing the cached value throws an {@code IOException}.
   */
  public void release(final K key) throws IOException {
    Preconditions.checkState(mIsOpen, "ReferenceCountedCache is closed.");
    Preconditions.checkNotNull(key);
    CacheEntry<V> entry = mMap.get(key);
    Preconditions.checkState(entry != null, "No cached value for key '%s'.", key);

    synchronized (entry) {
      if (entry.decrementAndGetCount() == 0) {
        // We need to remove the entry from the cache and clean up the value.
        mMap.remove(key);
        final V value = entry.getValue();
        if (mTracking) {
          ResourceTracker.get().unregisterResource(value);
        }
        value.close();
      }
    }
  }

  /**
   * Returns whether this cache contains <i>or</i> contained a cached entry for the provided key.
   *
   * @param key to check for a cached entry.
   * @return whether this cache contains <i>or</i> contained a cached entry for the key.
   */
  public boolean containsKey(K key) {
    Preconditions.checkNotNull(key);
    return mMap.containsKey(key);
  }

  /**
   * Make a best effort at closing all the cached values. This method is *not* guaranteed to close
   * every cached value if there are concurrent users of the cache.  As a result, this method
   * should only be relied upon if only a single thread is using this cache while {@code #close} is
   * called.
   *
   * @throws IOException if any entry throws an IOException while closing. An entry throwing an
   *    IOException will prevent any further entries from being close.
   */
  @Override
  public void close() throws IOException {
    mIsOpen = false;
    for (Map.Entry<K, CacheEntry<V>> entry : mMap.entrySet()) {
      // Attempt to remove the entry from the cache
      if (mMap.remove(entry.getKey(), entry.getValue())) {
        // If successful, close the value
        CacheEntry<V> cacheEntry = entry.getValue();
        synchronized (cacheEntry) {
          final V value = cacheEntry.getValue();
          if (mTracking) {
            ResourceTracker.get().unregisterResource(value);
          }
          cacheEntry.getValue().close();
        }
      }
    }
  }

  /**
   * A cache entry which includes a value and a reference count.
   */
  @NotThreadSafe
  private static final class CacheEntry<V> {
    private V mValue;
    private int mCount = 0;

    /**
     * Set the value of this entry. Should only be called a single time.
     *
     * @param value to set entry.
     * @return this.
     */
    public CacheEntry<V> setValue(final V value) {
      Preconditions.checkState(mValue == null, "Value already set.");
      mValue = Preconditions.checkNotNull(value);
      return this;
    }

    /**
     * Returns the value held by this cache entry.
     *
     * @return the value.
     */
    public V getValue() {
      Preconditions.checkState(mValue != null, "Value not yet set.");
      return mValue;
    }

    /**
     * Increment the reference count and return the new count.
     *
     * @return the newly incremented count.
     */
    public int incrementAndGetCount() {
      return ++mCount;
    }

    /**
     * Decrement the reference count and return the new count.
     *
     * @return the newly decremented count.
     */
    public int decrementAndGetCount() {
      return --mCount;
    }

    /**
     * Get the reference count.
     *
     * @return the reference count.
     */
    public int getCount() {
      return mCount;
    }
  }
}
