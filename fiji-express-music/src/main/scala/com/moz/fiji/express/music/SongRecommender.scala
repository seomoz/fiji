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

import com.twitter.scalding._

import com.moz.fiji.express.flow._
import com.moz.fiji.express.music.avro._

/**
 * Generates recommendations for the next song each user might like to listen to.
 *
 * For each user, we write a recommendation for the next song into
 * the info:next_song_rec column, based on the most recent song recorded in info:track_plays.
 * We incorporate the information we generated about popular sequences of songs by joining
 * tuples in the recommendedNextSongs pipe with the tuples in the main pipe on the songId
 * and lastTrackPlayed fields.
 *
 * @param args passed to this job from the command line.
 */
class SongRecommender(args: Args) extends FijiJob(args) {
  /**
   * This method retrieves the most popular song (at index 0) in the TopNextSongs record.
   *
   * @param songs from the TopNextSongs record.
   * @return the most popular song.
   */
  def getMostPopularSong(songs: Seq[FlowCell[TopSongs]]): String = {
    songs.head.datum.getTopSongs.get(0).getSongId.toString
  }

  /**
   * This Scalding RichPipe does the following:
   * 1. Reads the column "info:top_next_songs" from the songs table and emits a tuple for
   *     every row.
   * 2. Retrieves the most popular song played (in the 'nextSong field) after every given
   *     song (in the 'songId field.)
   * 3. Emits tuples containing only the fields 'songId and 'nextSong.
   */
  val recommendedSong = FijiInput.builder
      .withTableURI(args("songs-table"))
      .withColumnSpecs(QualifiedColumnInputSpec.builder
          .withColumn("info", "top_next_songs")
          .withSchemaSpec(SchemaSpec.Specific(classOf[TopSongs]))
          .build -> 'topNextSongs)
      .build
      .map('entityId -> 'songId) { eId: EntityId => eId(0) }
      .map('topNextSongs -> 'nextSong) { getMostPopularSong }
      .project('songId, 'nextSong)

  /**
   * This Scalding pipeline does the following:
   * 1. Reads the column "info:track_plays" from the users table.
   * 2. Retrieves the song most recently played by a user.
   * 3. Retrieve the TopNextSongs associated with the most recently played song by joining
   *     together the tuples emitted from the nextSongs pipe with the the 'lastTrackPlayed
   *     field.
   */
  FijiInput.builder
      .withTableURI(args("users-table"))
      .withColumnSpecs(QualifiedColumnInputSpec.builder
          .withColumn("info", "track_plays")
          .build -> 'trackPlays)
      .build
      .map('trackPlays -> 'lastTrackPlayed) {
          slice: Seq[FlowCell[CharSequence]] => slice.head.datum.toString }
      .joinWithSmaller('lastTrackPlayed -> 'songId, recommendedSong)
      .write(FijiOutput.builder
          .withTableURI(args("users-table"))
          .withColumns('nextSong -> "info:next_song_rec")
          .build)
}
