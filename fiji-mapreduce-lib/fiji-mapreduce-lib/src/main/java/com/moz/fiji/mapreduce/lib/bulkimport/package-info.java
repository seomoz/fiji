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

/**
 * Bulk importers for FijiMR.
 *
 * <p>
 *   Bulk importers are used for parsing data from input sources into fields which correspond to
 *   Fiji columns.  Bulk import jobs in FijiMR can be created using the
 *   <code>FijiBulkImportJobBuilder</code>.  There are two options for importing data
 *   into Fiji tables: using a bulk importer to create HFiles or "putting" individual rows. A bulk
 *   importer is suitable for reading in data from files, whereas putting is suitable for importing
 *   individual rows.  Bulk importers can be invoked using the <code>fiji bulk-import</code> tool.
 *   Generated HFiles from bulk importers can subsequently be loaded using the
 *   <code>fiji bulk-load</code> tool.
 * </p>
 *
 * <h2>Usable bulk importers:</h2>
 * <ul>
 * <li>{@link com.moz.fiji.mapreduce.lib.bulkimport.CommonLogBulkImporter} - Common Log bulk
 *     importer</li>
 * <li>{@link com.moz.fiji.mapreduce.lib.bulkimport.CSVBulkImporter} - CSV (Comma Separated Value)
 *     bulk importer that also processes TSV(Tab Separated Values).</li>
 * <li>{@link com.moz.fiji.mapreduce.lib.bulkimport.JSONBulkImporter} - JSON bulk importer
 *
 * <h2>Related Documentation:</h2>
 * <li>{@link com.moz.fiji.mapreduce.lib.bulkimport.DescribedInputTextBulkImporter} - Base class for
 *     bulk importing of any form of structured text class.  Other bulk importers will inherit
 *     from this including:</li>
 * <li>{@link com.moz.fiji.mapreduce.lib.bulkimport.FijiTableImportDescriptor} - The bulk import
 *     mapping import configuration.</li>
 * </ul>
 */

package com.moz.fiji.mapreduce.lib.bulkimport;
