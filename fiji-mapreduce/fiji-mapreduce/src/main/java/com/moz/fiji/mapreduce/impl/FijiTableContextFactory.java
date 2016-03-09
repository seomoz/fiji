/**
 * (c) Copyright 2012 WibiData, Inc.
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

package com.moz.fiji.mapreduce.impl;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;

/** Instantiates a FijiTableContext according to a configuration. */
@ApiAudience.Private
public final class FijiTableContextFactory {

  /**
   * Instantiates the configured FijiTableContext.
   *
   * @param taskContext Hadoop task context.
   * @return the configured FijiTableContext.
   * @throws IOException on I/O error.
   */
  public static FijiTableContext create(TaskInputOutputContext taskContext)
      throws IOException {
    final Configuration conf = taskContext.getConfiguration();
    final String className = conf.get(FijiConfKeys.FIJI_TABLE_CONTEXT_CLASS);
    if (className == null) {
      throw new IOException(String.format(
          "FijiTableContext class missing from configuration (key '%s').",
          FijiConfKeys.FIJI_TABLE_CONTEXT_CLASS));
    }

    Throwable throwable = null;
    try {
      final Class<?> genericClass = Class.forName(className);
      final Class<? extends FijiTableContext> klass =
          genericClass.asSubclass(FijiTableContext.class);
      final Constructor<? extends FijiTableContext> constructor =
          klass.getConstructor(TaskInputOutputContext.class);
      final FijiTableContext context = constructor.newInstance(taskContext);
      return context;
    } catch (ClassCastException cce) {
      throwable = cce;
    } catch (ClassNotFoundException cnfe) {
      throwable = cnfe;
    } catch (NoSuchMethodException nsme) {
      throwable = nsme;
    } catch (InvocationTargetException ite) {
      throwable = ite;
    } catch (IllegalAccessException iae) {
      throwable = iae;
    } catch (InstantiationException ie) {
      throwable = ie;
    }
    throw new IOException(
        String.format("Error instantiating FijiTableWriter class '%s': %s.",
            className, throwable.getMessage()),
        throwable);
  }

  /** Utility class cannot be instantiated. */
  private FijiTableContextFactory() {
  }
}
