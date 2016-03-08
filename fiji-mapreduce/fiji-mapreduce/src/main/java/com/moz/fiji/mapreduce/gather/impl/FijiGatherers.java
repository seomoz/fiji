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
package com.moz.fiji.mapreduce.gather.impl;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.mapreduce.gather.FijiGatherer;

/** Utility methods for working with <code>FijiGatherer</code>. */
@ApiAudience.Private
public final class FijiGatherers {
  /** Utility class, cannot be instantiated. */
  private FijiGatherers() {}

  /**
   * Loads a FijiGatherer class by name.
   *
   * @param className Fully qualified name of the class to load.
   * @return the loaded class.
   * @throws ClassNotFoundException if the class is not found.
   * @throws ClassCastException if the class is not a FijiBulkImporter.
   */
  @SuppressWarnings("rawtypes")
  public static Class<? extends FijiGatherer> forName(String className)
      throws ClassNotFoundException {
    return Class.forName(className).asSubclass(FijiGatherer.class);
  }
}
