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

package com.moz.fiji.mapreduce;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;

import com.moz.fiji.schema.layout.FijiTableLayouts;

/** Testing resources. */
public final class TestingResources {
  /**
   * Loads a text resource by name.
   *
   * @param resourcePath Path of the resource to load.
   * @return the resource content, as a string.
   * @throws IOException on I/O error.
   */
  public static String get(final String resourcePath) throws IOException {
    final InputStream istream =
        FijiTableLayouts.class.getClassLoader().getResourceAsStream(resourcePath);
    try {
      return IOUtils.toString(istream);
    } finally {
      istream.close();
    }
  }

  /**
   * Writes a text file.
   *
   * @param path Path of the file to write.
   * @param content Text content of the file to create.
   * @throws IOException on I/O error.
   */
  public static void writeTextFile(final File path, final String content) throws IOException {
    final FileOutputStream ostream = new FileOutputStream(path);
    try {
      IOUtils.write(content, ostream);
    } finally {
      ostream.close();
    }
  }

  /**
   * Reads a resource and writes it to a local temporary file.
   *
   * @param resourcePath Path of the resource to load.
   * @param directory Path to the directory to write the temporary file to.
   * @return A handle to the written temporary file.
   * @throws IOException on I/O error.
   */
  public static File getResourceAsTempFile(
      final String resourcePath,
      final File directory
  ) throws IOException {
    final File tempFile = File.createTempFile("TempResourceFile", "", directory);
    TestingResources.writeTextFile(tempFile, TestingResources.get(resourcePath));
    return tempFile;
  }

  /** Utility class may not be instantiated. */
  private TestingResources() {
  }
}
