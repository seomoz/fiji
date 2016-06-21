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

package com.moz.fiji.schema;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;

/**
 * Interface for scanning over rows read from a Fiji table.
 *
 * <p>
 *   The row scanner behaves like an iterator over the rows in a Fiji table.
 *   The scanner implements the Java Iterable interface, so you can write code like this:
 * </p>
 *   <pre>{@code
 *     FijiRowScanner scanner = tableReader.getScanner(...);
 *     try {
 *       for (FijiRowData row : scanner) {
 *         process(row);
 *       }
 *     } finally {
 *       // Always close resources:
 *       scanner.close();
 *     }
 *   }</pre>
 *
 * <p>
 *   Note: the scanner is backed by a single iterator:
 *   {@link FijiRowScanner#iterator()} returns the same iterator always.
 *   As a result, the scanner and its iterator should not be used simultaneously from multiple
 *   places (eg. from multiple threads).
 *   Similarly, when chaining two for loops on the same row scanner, the second for loop will
 *   restart where the first for loop stopped.
 * </p>.
 */
@ApiAudience.Public
@ApiStability.Evolving
@Inheritance.Sealed
public interface FijiRowScanner extends Closeable, Iterable<FijiRowData> {

  /**
   * Returns the iterator over Fiji rows backing this scanner.
   *
   * <p>
   *   Always returns the same iterator.
   *   As a result, the scanner and its iterator should not be used simultaneously from multiple
   *   places (eg. from multiple threads).
   *   Similarly, when chaining two for loops on the same row scanner, the second for loop will
   *   restart where the first for loop stopped.
   * </p>
   *
   * @return the iterator over Fiji rows backing this scanner.
   */
  @Override
  Iterator<FijiRowData> iterator();

  /**
   * Closes this scanner and releases any system resources associated with it.
   *
   * <p>Calling this method when you are finished with the scanner is important.
   * See http://hbase.apache.org/book.html#perf.hbase.client.scannerclose for details.</p>
   *
   * @throws IOException If an I/O error occurs.
   */
  @Override
  void close() throws IOException;
}
