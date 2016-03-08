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

package com.moz.fiji.examples.music;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.moz.fiji.examples.music.produce.NextSongRecommender;
import com.moz.fiji.mapreduce.FijiMapReduceJob;
import com.moz.fiji.mapreduce.MapReduceJobOutput;
import com.moz.fiji.mapreduce.kvstore.lib.FijiTableKeyValueStore;
import com.moz.fiji.mapreduce.output.MapReduceJobOutputs;
import com.moz.fiji.mapreduce.produce.FijiProduceJobBuilder;
import com.moz.fiji.schema.FijiClientTest;
import com.moz.fiji.schema.FijiDataRequest;
import com.moz.fiji.schema.FijiDataRequestBuilder;
import com.moz.fiji.schema.FijiTable;
import com.moz.fiji.schema.FijiTableReader;
import com.moz.fiji.schema.FijiURI;
import com.moz.fiji.schema.layout.FijiTableLayout;
import com.moz.fiji.schema.util.InstanceBuilder;
import com.moz.fiji.schema.util.ResourceUtils;

/**
 * Tests our recommendation producer.
 */
public class TestNextSongRecommender extends FijiClientTest {

  private FijiURI mUserTableURI;
  private FijiURI mSongTableURI;
  private FijiTable mUserTable;
  private FijiTableReader mUserTableReader;

  /** Initialize our environment. */
  @Before
  public final void setup() throws Exception {
    // Create layouts and URIs to use during the test.
    final FijiTableLayout userLayout =
        FijiTableLayout.createFromEffectiveJsonResource("/layout/users.json");
    final String userTableName = userLayout.getName();
    mUserTableURI = FijiURI.newBuilder(getFiji().getURI()).withTableName(userTableName).build();
    final FijiTableLayout songLayout =
        FijiTableLayout.createFromEffectiveJsonResource("/layout/songs.json");
    final String songTableName = songLayout.getName();
    mSongTableURI = FijiURI.newBuilder(getFiji().getURI()).withTableName(songTableName).build();

    SongCount songFour = new SongCount();
    songFour.setCount(10L);
    songFour.setSongId("song-4");
    List<SongCount> listOfSongFour = Lists.newArrayList(songFour);
    TopSongs topSongsForSong1 = new TopSongs();
    topSongsForSong1.setTopSongs(listOfSongFour);

    SongCount songFive = new SongCount();
    songFive.setCount(9L);
    songFive.setSongId("song-5");
        List<SongCount> listOfSongFive = Lists.newArrayList(songFive);
    TopSongs topSongsForSong2 = new TopSongs();
    topSongsForSong2.setTopSongs(listOfSongFive);
    // Initialize a fiji instance with relevant tables to use during tests.
    new InstanceBuilder(getFiji())
        .withTable(userTableName, userLayout)
            .withRow("user-1").withFamily("info").withQualifier("track_plays")
                .withValue(2L, "song-2")
                .withValue(3L, "song-1")
            .withRow("user-2").withFamily("info").withQualifier("track_plays")
                .withValue(8L, "song-1")
                .withValue(9L, "song-3")
                .withValue(10L, "song-2")
        .withTable(songLayout.getName(), songLayout)
            .withRow("song-1").withFamily("info").withQualifier("top_next_songs")
                .withValue(1L, topSongsForSong1)
            .withRow("song-2").withFamily("info").withQualifier("top_next_songs")
                .withValue(1L, topSongsForSong2)
        .build();
    // Open table and table reader.
    mUserTable = getFiji().openTable(userTableName);
    mUserTableReader = mUserTable.openTableReader();
  }

  /**  Close resources we open for the test. */
  @After
  public final void cleanup() {
    // Close table and table reader in the reverse order.
    ResourceUtils.closeOrLog(mUserTableReader);
    ResourceUtils.releaseOrLog(mUserTable);
  }

  @Test
  public void testProducer() throws IOException, ClassNotFoundException, InterruptedException {
    MapReduceJobOutput tableOutput =
        MapReduceJobOutputs.newDirectFijiTableMapReduceJobOutput(mUserTableURI, 1);
    FijiTableKeyValueStore.Builder kvStoreBuilder = FijiTableKeyValueStore.builder();
    // Our default implementation will use the default fiji instance, and a table named songs.
    kvStoreBuilder.withColumn("info", "top_next_songs").withTable(mSongTableURI);

    // Configure first job.
    final FijiMapReduceJob mrjob = FijiProduceJobBuilder.create()
        .withConf(getConf())
        .withProducer(NextSongRecommender.class)
        .withInputTable(mUserTableURI)
        .withOutput(tableOutput)
        .withStore("nextPlayed", kvStoreBuilder.build())
        .build();

    // Run the job and confirm that it is successful.
    assertTrue(mrjob.run());

    FijiDataRequest request = FijiDataRequest.builder()
        .addColumns(FijiDataRequestBuilder.ColumnsDef.create()
            .withMaxVersions(Integer.MAX_VALUE)
            .add("info", "next_song_rec"))
        .build();

    CharSequence valueForSong1 = mUserTableReader.get(mUserTable.getEntityId("user-1"), request)
        .getMostRecentValue("info", "next_song_rec");
    assertEquals("User-1 just listened to son-1, so their next song rec should be song-4", "song-4",
        valueForSong1.toString());

  }
}
