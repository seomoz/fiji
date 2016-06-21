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
package com.moz.fiji.examples.music.bulkimport;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.moz.fiji.examples.music.FijiMusicCounters;
import com.moz.fiji.examples.music.SongMetadata;
import com.moz.fiji.mapreduce.FijiTableContext;
import com.moz.fiji.mapreduce.bulkimport.FijiBulkImporter;
import com.moz.fiji.schema.EntityId;

/**
 * Bulk-importer to load the FijiMusic songs metadata from a text file into a Fiji table.
 *
 * <p>
 * Input files will contain JSON data representing song metadata, with one song per line:
 * </p>
 * <pre>
 * {
 *   "song_id" : "0",
 *   "song_name" : "song0",
 *   "artist_name" : "artist1",
 *   "album_name" : "album1",
 *   "genre" : "awesome",
 *   "tempo" : "140",
 *   "duration" : "180"
 * }
 * </pre>
 *
 * <p>
 * The bulk-importer expects a text input format:
 * </p>
 * <ul>
 *   <li> input keys are the positions (in bytes) of each line in input file;</li>
 *   <li> input values are the lines, as Text instances.</li>
 * </ul>
 */
public class SongMetadataBulkImporter extends FijiBulkImporter<LongWritable, Text> {
  private static final Logger LOG = LoggerFactory.getLogger(SongMetadataBulkImporter.class);

  /** {@inheritDoc} */
  @Override
  public void produce(LongWritable filePos, Text line, FijiTableContext context)
      throws IOException {

    final JSONParser parser = new JSONParser();
    try {
      // Parse JSON:
      final JSONObject json = (JSONObject) parser.parse(line.toString());

      // Extract JSON fields:
      final String songId = json.get("song_id").toString();
      final String songName = json.get("song_name").toString();
      final String artistName = json.get("artist_name").toString();
      final String albumName = json.get("album_name").toString();
      final String genre = json.get("genre").toString();
      final long tempo = Long.valueOf(json.get("tempo").toString());
      final long duration = Long.valueOf(json.get("duration").toString());

      // Build Avro metadata record:
      final EntityId eid = context.getEntityId(songId);
      final SongMetadata song = SongMetadata.newBuilder()
          .setSongName(songName)
          .setAlbumName(albumName)
          .setArtistName(artistName)
          .setGenre(genre)
          .setTempo(tempo)
          .setDuration(duration)
          .build();

      // Write entity to Fiji:
      context.put(eid, "info", "metadata", song);

    } catch (ParseException pe) {
      // Catch and log any malformed JSON records.
      context.incrementCounter(FijiMusicCounters.JSONParseFailure);
      LOG.error("Failed to parse JSON record '{}': {}", line, pe);
    }
  }
}
