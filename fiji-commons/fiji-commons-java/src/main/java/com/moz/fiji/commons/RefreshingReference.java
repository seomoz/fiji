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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RefreshingReference serves as an object cache, containing a
 * reference to a single object that is periodically refreshed
 * asynchronously until closed.
 *
 * @param <T> The type that will be cached.
 */
public final class RefreshingReference<T> implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshingReference.class);

  /**
   * Atomic reference to the cached value, which is wrapped in a WithTimestamp.
   */
  private final AtomicReference<WithTimestamp<T>> mRef;

  /**
   * Scheduler used for periodically refreshing the cached value.
   */
  private final ScheduledExecutorService mScheduler;

  /**
   * Used to initialize and refresh the cached value.
   */
  private final RefreshingLoader<T> mRefreshingLoader;

  /**
   * Used to deterministically control refresh cycles during testing.
   */
  private final Optional<LoopController> mLoopController;

  /**
   * Static factory constructor. Allows the user to pass in a name for the thread
   * the scheduler will run on.
   *
   * @param refreshPeriod Configures the refresh rate for the scheduler.
   * @param timeUnit Specifies the unit of time for the refreshPeriod.
   * @param refreshingLoader Used to initialize and refresh the cached value.
   * @param name The name of the thread the scheduler will run on.
   * @param loopController Used to deterministically control refresh cycles during testing.
   * @param <U> The type that will be cached.
   * @return A new RefreshingReference.
   */
  public static <U> RefreshingReference<U> create(
      final Long refreshPeriod,
      final TimeUnit timeUnit,
      final RefreshingLoader<U> refreshingLoader,
      final String name,
      final Optional<LoopController> loopController
  ) {
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder()
        .setNameFormat(name + "-%d");
    return new RefreshingReference<>(
        refreshPeriod,
        timeUnit,
        refreshingLoader,
        builder,
        loopController);
  }

  /**
   * Static factory constructor. Provides a default name for the thread the scheduler will
   * run on.
   *
   * @param refreshPeriod Configures the refresh rate for the scheduler.
   * @param timeUnit Specifies the unit of time for the refreshPeriod.
   * @param refreshingLoader Used to initialize and refresh the cached value.
   * @param loopController Used to deterministically control refresh cycles during testing.
   * @param <U> The type that will be cached.
   * @return A new RefreshingReference.
   */
  public static <U> RefreshingReference<U> create(
      final Long refreshPeriod,
      final TimeUnit timeUnit,
      final RefreshingLoader<U> refreshingLoader,
      final Optional<LoopController> loopController
  ) {
    ThreadFactoryBuilder builder = new ThreadFactoryBuilder()
        .setNameFormat("refreshing-reference-%d");
    return new RefreshingReference<>(
        refreshPeriod,
        timeUnit,
        refreshingLoader,
        builder,
        loopController);
  }

  /**
   * Private constructor.
   *
   * @param refreshPeriod Configures the refresh rate for the scheduler.
   * @param timeUnit Specifies the unit of time for the refreshPeriod.
   * @param refreshingLoader Used to initialize and refresh the cached value.
   * @param builder Thread factory builder. Expects that the name format is already set.
   * @param loopController Used to deterministically control refresh cycles during testing.
   */
  private RefreshingReference(
      final Long refreshPeriod,
      final TimeUnit timeUnit,
      final RefreshingLoader<T> refreshingLoader,
      final ThreadFactoryBuilder builder,
      final Optional<LoopController> loopController
  ) {
    mRef = new AtomicReference<>(WithTimestamp.create(refreshingLoader.initial()));
    mScheduler = Executors.newSingleThreadScheduledExecutor(
        builder
            .setDaemon(true)
            .build()
    );
    mRefreshingLoader = refreshingLoader;
    final RefreshingRunnable runnable = new RefreshingRunnable();
    mScheduler.scheduleAtFixedRate(runnable, refreshPeriod, refreshPeriod, timeUnit);
    mLoopController = loopController;
    ResourceTracker.get().registerResource(this);
  }

  /**
   * Runnable implementation used to refresh the reference.
   */
  private class RefreshingRunnable implements Runnable {

    /**
     * {@inheritDoc}
     *
     * Refreshes the reference. In the case of a checked exception being thrown in the
     * refresh method, the error is logged and the cache will contain the old value. If
     * refresh throws an unchecked exception, it will be logged and rethrown.
     */
    @Override
    public void run() {
      try {
        if (mLoopController.isPresent()) {
          mLoopController.get().await();
        }
        final T value = mRef.get().getValue();
        final T newValue = mRefreshingLoader.refresh(value);
        mRef.set(WithTimestamp.create(newValue));
      } catch (Exception e) {
        LOG.error("Refresh failed.", e);
      } catch (Throwable e) {
        LOG.error("Fatal refresh exception.", e);
        throw e;
      }
    }
  }

  /**
   * Returns the current value of this reference.
   * May be an old value if the scheduler has not refreshed the value recently.
   *
   * @return The current value of this reference.
   */
  public T get() {
    return mRef.get().getValue();
  }

  /**
   * Returns the current value of this reference wrapped in a WithTimestamp class, which
   * packages the value along with the time at which the WithTimestamp object was created
   * (Measured in milliseconds since the epoch). May be an old value if the scheduler has not
   * refreshed recently.
   *
   * @return Returns a reference to the cached object wrapped in a WithTimestamp class.
   */
  public WithTimestamp<T> getWithTimestamp() {
    return mRef.get();
  }

  /**
   * Shuts down the scheduler and calls the close method in the RefreshingLoader.
   * Allows the user to demand immediate shutdown; this should only be used for testing.
   *
   * @param closeNow Will shut down immediately if this is true, otherwise will attempt to wait
   *     for concurrent execution.
   * @throws IOException If the RefreshingLoader encounters an error on closing.
   */
  public void close(
      Boolean closeNow
  ) throws IOException {
    try {
      if (closeNow) {
        mScheduler.shutdownNow();
        mRefreshingLoader.close();
      } else {
        mScheduler.shutdown();
        // Await termination so that we don't close the state out from under the function
        mScheduler.awaitTermination(10, TimeUnit.SECONDS);
        mRefreshingLoader.close();
      }
    } catch (InterruptedException e) {
      Thread.interrupted();
      mRefreshingLoader.close();
    } finally {
      ResourceTracker.get().unregisterResource(this);
    }
  }

  /**
   * Shuts down the scheduler and calls the close method in the RefreshingLoader. Attempts
   * to wait for concurrent execution, but does not wait indefinitely.
   *
   * @throws IOException If the RefreshingLoader encounters an error on closing.
   */
  public void close() throws IOException {
    close(false);
  }
}
