/**
 * (c) Copyright 2013 WibiData, Inc.
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

package com.moz.fiji.mapreduce.pivot.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;
import com.moz.fiji.mapreduce.pivot.FijiPivoter;

/** Utility methods for working with {@link FijiPivoter}. */
@ApiAudience.Private
public final class FijiPivoters {
  /** Utility class. */
  private FijiPivoters() {}

  /**
   * Create an instance of {@link FijiPivoter} as specified from a given
   * {@link org.apache.hadoop.conf.Configuration}.
   *
   * @param conf The job configuration.
   * @return a new {@link FijiPivoter} instance.
   * @throws IOException if the class cannot be loaded.
   */
  public static FijiPivoter create(Configuration conf) throws IOException {
    final Class<? extends FijiPivoter> tableMapperClass =
        conf.getClass(FijiConfKeys.FIJI_PIVOTER_CLASS, null, FijiPivoter.class);
    if (null == tableMapperClass) {
      throw new IOException("Unable to load pivoter class");
    }
    return ReflectionUtils.newInstance(tableMapperClass, conf);
  }

  /**
   * Loads a {@link FijiPivoter} class by name.
   *
   * @param className Fully qualified name of the class to load.
   * @return the loaded class.
   * @throws ClassNotFoundException if the class is not found.
   * @throws ClassCastException if the class is not a {@link FijiPivoter}.
   */
  public static Class<? extends FijiPivoter> forName(String className)
      throws ClassNotFoundException {
    return Class.forName(className).asSubclass(FijiPivoter.class);
  }
}
