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

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.commons.ReferenceCountable;
import com.moz.fiji.schema.layout.FijiTableLayout;

/**
 * The FijiTable interface provides operations on FijiTables. To perform reads to and
 * writes from a Fiji table use a {@link FijiTableReader} or {@link FijiTableWriter}.
 * Instances of these classes can be obtained by using the {@link #openTableReader()}
 * and {@link #openTableWriter()} methods.  {@link EntityId}s, which identify particular
 * rows within a Fiji table are also generated from its components.
 *
 * <h2>FijiTable instance lifecycle:</h2>
 * <p>
 *   To open a connection to a FijiTable, use {@link Fiji#openTable(String)}. A FijiTable
 *   contains an open connection to an HBase cluster. Because of this, FijiTable objects
 *   must be released using {@link #release()} when finished using it:
 * </p>
 * <pre>
 *   <code>
 *     final FijiTable table = myFiji.openTable("tableName");
 *     // Do some magic
 *     table.release();
 *   </code>
 * </pre>
 *
 * <h2>Reading &amp; Writing from a FijiTable:</h2>
 * <p>
 *   The FijiTable interface does not directly provide methods to perform I/O on a Fiji
 *   table. Read and write operations can be performed using either a {@link FijiTableReader}
 *   or a {@link FijiTableWriter}:
 * </p>
 * <pre>
 *   <code>
 *     final FijiTable table = myFiji.openTable("tableName");
 *
 *     final EntityId myId = table.getEntityId("myRowKey");
 *
 *     final FijiTableReader reader = table.openTableReader();
 *     final FijiTableWriter writer = table.openTableWriter();
 *
 *     // Read some data from a Fiji table using an existing EntityId and FijiDataRequest.
 *     final FijiRowData row = reader.get(myId, myDataRequest);
 *
 *     // Do things with the row...
 *
 *     // Write some data to a new column in the same row.
 *     writer.put(myId, "info", "newcolumn", "newvalue");
 *
 *     // Close open connections.
 *     reader.close();
 *     writer.close();
 *     table.release();
 *   </code>
 * </pre>
 *
 * @see FijiTableReader for more information about reading data from a Fiji table.
 * @see FijiTableWriter for more information about writing data to a Fiji table.
 * @see EntityId for more information about identifying rows with entity ids.
 * @see Fiji for more information about opening a FijiTable instance.
 */
@ApiAudience.Public
@ApiStability.Stable
@Inheritance.Sealed
public interface FijiTable extends ReferenceCountable<FijiTable> {
  /** @return the Fiji instance this table belongs to. */
  Fiji getFiji();

  /** @return the name of this table. */
  String getName();

  /** @return the URI for this table, trimmed at the table path component. */
  FijiURI getURI();

  /** @return the layout of this table. */
  FijiTableLayout getLayout();

  /**
   * Creates an entity id from a list of components.
   *
   * @param fijiRowKey This can be one of the following depending on row key encoding:
   *     <ul>
   *       <li>
   *         Raw, Hash, Hash-Prefix EntityId: A single String or byte array
   *         component.
   *       </li>
   *       <li>
   *         Formatted EntityId: The primitive row key components (string, int,
   *         long) either passed in their expected order in the key or as an ordered
   *         list of components.
   *       </li>
   *     </ul>
   * @return a new EntityId with the specified Fiji row key.
   */
  EntityId getEntityId(Object... fijiRowKey);

  /**
   * Opens a FijiTableReader for this table.
   *
   * <p>
   *   This method is equivalent to <code>getReaderFactory().readerBuilder().build()</code>. It sets
   *   all options to their default values.
   * </p>
   *
   * <p> The caller of this method is responsible for closing the returned reader. </p>
   * <p> The reader returned by this method does not provide any isolation guarantee.
   *     In particular, you should assume that the underlying resources (connections, buffers, etc)
   *     are used concurrently for other purposes. </p>
   *
   * @return A FijiTableReader for this table.
   * @throws FijiIOException Future implementations may throw unchecked FijiIOException.
   */
  FijiTableReader openTableReader();

  /**
   * Gets a FijiReaderFactory for this table.
   *
   * <p> The returned reader factory is valid as long as the caller retains the table. </p>
   *
   * @throws IOException in case of an error.
   * @return A FijiReaderFactory.
   */
  FijiReaderFactory getReaderFactory() throws IOException;

  /**
   * Opens a FijiTableWriter for this table.
   *
   * <p> The caller of this method is responsible for closing the returned writer.
   * <p> The writer returned by this method does not provide any isolation guarantee.
   *     In particular, you should assume that the underlying resources (connections, buffers, etc)
   *     are used concurrently for other purposes.
   * <p> If you have specific resource requirements, such as buffering, timeouts, dedicated
   *     connection, etc, use {@link #getWriterFactory()}.
   *
   * @return A FijiTableWriter for this table.
   * @throws FijiIOException Future implementations may throw unchecked FijiIOException.
   */
  FijiTableWriter openTableWriter();

  /**
   * Gets a FijiWriterFactory for this table.
   *
   * <p> The returned writer factory is valid as long as the caller retains the table. </p>
   *
   * @throws IOException in case of an error.
   * @return A FijiWriterFactory.
   */
  FijiWriterFactory getWriterFactory() throws IOException;

  /**
   * Return the regions in this table as an ordered list.
   *
   * @return An ordered list of the table regions.
   * @throws IOException If there is an error retrieving the regions of this table.
   * @deprecated Use {@link #getPartitions()}.
   */
  @Deprecated
  List<FijiRegion> getRegions() throws IOException;

  /**
   * Get the complete and non-overlapping group of partitions in the table.
   *
   * <p>
   *   These partitions may be used to perform efficient scans over the rows of the table.
   *   Performing a scan for each returned partition will yield all of the rows in the table exactly
   *   once.
   * </p>
   *
   * @return The partitions of this Fiji table.
   * @throws IOException If there is an error retrieving the partitions of this table.
   */
  Collection<? extends FijiPartition> getPartitions() throws IOException;

  /**
   * Get a table and column annotator for this table.
   *
   * @return a table and column annotator for this table.
   * @throws IOException in case of an error getting the meta table.
   */
  FijiTableAnnotator openTableAnnotator() throws IOException;
}
