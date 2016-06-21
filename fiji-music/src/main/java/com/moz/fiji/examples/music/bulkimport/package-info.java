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
 * This package contains the bulk-importers used in the FijiMusic tutorial.
 *
 * <p>
 * Bulk imports are run from the shell:
 * </p>
 * <pre>
 *   fiji bulk-import \
 *       --fiji=${FIJI} \
 *       --importer=com.moz.fiji.examples.music.bulkimport.SongMetadataBulkImporter \
 *       --lib=lib/ \
 *       --output="format=fiji table=${FIJI}/songs nsplits=1" \
 *       --input="format=text file=${HDFS_ROOT}/fiji-mr-tutorial/song-metadata.json"
 * </pre>
 *
 * <p>
 * The two classes in this package are very similar.  Both import data from a text file
 * into a Fiji table for processing later in the tutorial.  The only difference is the
 * expected JSON format of the data.  One imports song metadata and another the data on
 * song plays by users.
 * </p>
 */
package com.moz.fiji.examples.music.bulkimport;
