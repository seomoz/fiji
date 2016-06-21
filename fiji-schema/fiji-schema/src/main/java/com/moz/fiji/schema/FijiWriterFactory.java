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

package com.moz.fiji.schema;

import java.io.IOException;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;

/**
 * Interface for table writer factories.
 *
 * <p> Use <code>FijiTable.getWriterFactory()</code> to get a writer.
 */
@ApiAudience.Public
@ApiStability.Evolving
public interface FijiWriterFactory {

  /**
   * Opens a new FijiTableWriter for the FijiTable associated with this writer factory.
   * The caller of this method is responsible for closing the writer.
   *
   * @return A new FijiTableWriter.
   * @throws IOException in case of an error.
   */
  FijiTableWriter openTableWriter() throws IOException;

  /**
   * Opens a new AtomicFijiPutter for the FijiTable associated with this writer factory.
   * The caller of this method is responsible for closing the writer.
   *
   * @return A new AtomicFijiPutter.
   * @throws IOException in case of an error.
   */
  AtomicFijiPutter openAtomicPutter() throws IOException;

  /**
   * Opens a new FijiBufferedWriter for the FijiTable associated with this writer factory.
   * The caller of this method is responsible for closing the writer.
   *
   * @return A new FijiBufferedWriter.
   * @throws IOException in case of an error.
   */
  FijiBufferedWriter openBufferedWriter() throws IOException;
}
