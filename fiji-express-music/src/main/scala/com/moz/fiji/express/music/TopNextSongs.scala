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

package com.moz.fiji.express.music

import scala.collection.JavaConverters.seqAsJavaListConverter

import com.twitter.scalding._

import com.moz.fiji.express.flow._
import com.moz.fiji.express.music.avro._

/**
 * For each song S, create a list of songs sorted by the number of times a song was played
 * after S.
 *
 * This job accepts two command line arguments, `--users-table` and `--songs-table` that
 * should be set to the Fiji URIs of a users and songs table in Fiji. The play histories of
 * users (stored in column `info:track_plays`) are used to compute how many times each song
 * is played after another. The top next songs for each song are written to the column
 * `info:top_next_songs` of the songs table.
 *
 * @param args passed from the command line.
 */
class TopNextSongs(args: Args) extends FijiJob(args) {
  /**
   * Transforms a slice of song ids into a collection of tuples `(s1, s2)` signifying that `s2`
   * appeared after `s1` in the slice, chronologically.
   *
   * @param slice of song ids representing a user's play history.
   * @return a list of song bigrams.
   */
  def bigrams(slice: Seq[FlowCell[String]]): List[(String, String)] = {
    slice
        .sortBy { _.version }
        .sliding(2)
        .map { itr => itr.iterator }
        .map { itr => (itr.next().datum, itr.next().datum) }
        .toList
  }

  /**
   * Transforms a group of tuples into a group containing a list of song count records,
   * sorted by count.
   *
   * @param nextSongs is the group of tuples containing song count records.
   * @return a group containing a list of song count records, sorted by count.
   */
  def sortNextSongs(nextSongs: GroupBuilder): GroupBuilder = {
    nextSongs.sortBy('count).reverse.toList[SongCount]('song_count -> 'top_songs)
  }

  // This Scalding pipeline does the following:
  // 1. Reads the column "info:track_plays" from a users table in Fiji.
  // 2. Transforms each user's play history into a collection of bigrams that record when
  //    one song was played after another.
  // 3. Counts the number of times each song was played after another.
  // 4. Creates a song count Avro record from each bigram.
  // 5. For each song S, creates a list of songs sorted by the number of times the song was
  //    played after S.
  // 6. Converts each list of SongCount records into an Avro-compatible java.util.List.
  // 7. Packs each list into an Avro record.
  // 8. Creates an entity id for the songs table for each song.
  // 9. Writes each song's TopSongs record to Fiji.
  FijiInput.builder
      .withTableURI(args("users-table"))
      .withColumnSpecs(QualifiedColumnInputSpec.builder
          .withColumn("info", "track_plays")
          .withMaxVersions(all)
          .build -> 'playlist)
      .build
      .flatMap('playlist -> ('first_song, 'song_id)) { bigrams }
      .groupBy(('first_song, 'song_id)) { _.size('count) }
      .pack[SongCount](('song_id, 'count) -> 'song_count)
      .groupBy('first_song) { sortNextSongs }
      .map('top_songs -> 'top_songs) { ts: List[SongCount] => ts.asJava }
      .pack[TopSongs]('top_songs -> 'top_next_songs)
      .map('first_song -> 'entityId) { firstSong: String => EntityId(firstSong) }
      .write(FijiOutput.builder
          .withTableURI(args("songs-table"))
          .withColumnSpecs('top_next_songs -> QualifiedColumnOutputSpec.builder
              .withColumn("info", "top_next_songs")
              .withSchemaSpec(SchemaSpec.Specific(classOf[TopSongs]))
              .build)
          .build)
}
