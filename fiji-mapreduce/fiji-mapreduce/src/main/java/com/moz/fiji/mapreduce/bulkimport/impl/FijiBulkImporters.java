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

package com.moz.fiji.mapreduce.bulkimport.impl;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImporter;
import com.moz.fiji.mapreduce.framework.FijiConfKeys;

/** Utility methods for working with <code>FijiBulkImporter</code>. */
@ApiAudience.Private
public final class FijiBulkImporters {
  /** Utility class. */
  private FijiBulkImporters() {}

  /**
   * <p>Create an instance of the bulk importer specified by the
   * {@link org.apache.hadoop.conf.Configuration}.</p>
   *
   * The configuration would have stored the bulk importer
   * name only if it was configured by a FijiBulkImportJob, so don't try
   * calling this method with any old Configuration object.
   *
   * @param <K> The map input key for the bulk importer.
   * @param <V> The map input value for the bulk importer.
   * @param conf The job configuration.
   * @return a brand-spankin'-new FijiBulkImporter instance.
   * @throws IOException If the bulk importer cannot be loaded.
   */
  @SuppressWarnings("unchecked")
  public static <K, V> FijiBulkImporter<K, V> create(Configuration conf) throws IOException {
    final Class<? extends FijiBulkImporter> bulkImporterClass =
        conf.getClass(FijiConfKeys.KIJI_BULK_IMPORTER_CLASS, null, FijiBulkImporter.class);
    if (null == bulkImporterClass) {
      throw new IOException("Unable to load bulk importer class");
    }

    return ReflectionUtils.newInstance(bulkImporterClass, conf);
  }

  /**
   * Loads a FijiBulkImporter class by name.
   *
   * @param className Fully qualified name of the class to load.
   * @return the loaded class.
   * @throws ClassNotFoundException if the class is not found.
   * @throws ClassCastException if the class is not a FijiBulkImporter.
   */
  @SuppressWarnings("rawtypes")
  public static Class<? extends FijiBulkImporter> forName(String className)
      throws ClassNotFoundException {
    return Class.forName(className).asSubclass(FijiBulkImporter.class);
  }
}
