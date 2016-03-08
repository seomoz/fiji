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

import com.moz.fiji.annotations.ApiAudience;
import com.moz.fiji.annotations.ApiStability;
import com.moz.fiji.annotations.Inheritance;
import com.moz.fiji.schema.avro.MetaTableBackup;
import com.moz.fiji.schema.layout.FijiTableLayoutDatabase;

/**
 * <p>
 * The Fiji meta data table, which stores layouts and other user defined meta data on a per-table
 * basis.
 *
 * Instantiated in FijiSchema via {@link com.moz.fiji.schema.Fiji#getMetaTable()}
 * </p>
 * @see FijiSchemaTable
 * @see FijiSystemTable
 */
@ApiAudience.Framework
@ApiStability.Evolving
@Inheritance.Sealed
public interface FijiMetaTable extends Closeable, FijiTableLayoutDatabase,
    FijiTableKeyValueDatabase<FijiMetaTable> {

  /**
   * Remove all metadata, including layouts, for a particular table.
   *
   * @param table The name of the fiji table to delete.
   * @throws IOException If there is an error.
   */
  void deleteTable(String table) throws IOException;

  /** {@inheritDoc} */
  @Override
  void close() throws IOException;

  /**
   * Returns metadata backup information in a form that can be directly written to a MetadataBackup
   * record. To read more about the avro type that has been specified to store this info, see
   * Layout.avdl
   *
   * @throws IOException If there is an error.
   * @return A map from table names to TableBackup records.
   */
  MetaTableBackup toBackup() throws IOException;

  /**
   * Restores metadata from a backup record. This consists of table layouts, schemas, and user
   * defined key-value pairs.
   *
   * @param backup A map from table name to table backup record.
   * @throws IOException on I/O error.
   */
  void fromBackup(MetaTableBackup backup) throws IOException;
}
