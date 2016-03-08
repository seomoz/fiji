---
layout: post
title: Importing Data
categories: [tutorials, express-recommendation, devel]
tags: [express-music]
order: 4
description: Importing data files into Fiji tables.
---


In this section of the tutorial, we will import metadata about songs into the Fiji table `songs`
and import data about when users have listened to songs into the Fiji table `users`.


### FijiExpress Custom Importers

Fiji provides stock [bulk importers]({{site.userguide_mapreduce_devel}}/bulk-importers/) that work for a
number of standard use cases. Because these importers can quickly become complicated once
you have to customize them, we've provided custom importers written in FijiExpress for importing the
user data and song metadata from JSON files on HDFS.

The source code for one of the importers is included at the [bottom of this page](#importer-source).

#### Importing Tutorial Data
For this example, we use command-line options specific to this job to specify the input JSON file
and the target Fiji table.

Run the the song metadata importer as a precompiled job contained in a JAR file:

<div class="userinput">
{% highlight bash %}
express.py job --jars="${MUSIC_EXPRESS_HOME}/lib/*" \
    --class=org.fiji.express.music.SongMetadataImporter \
    --mode=hdfs \
    --input express-tutorial/song-metadata.json \
    --table-uri ${KIJI}/songs
{% endhighlight %}
</div>

**Note that the job-specific flags ("input" and "table-uri") do not have a "=" between the flag names and their values.**

Use a similar command to import the user data:

<div class="userinput">
{% highlight bash %}
express.py job --jars="${MUSIC_EXPRESS_HOME}/lib/*" \
    --class=org.fiji.express.music.SongPlaysImporter \
    --mode=hdfs \
    --input express-tutorial/song-plays.json \
    --table-uri ${KIJI}/users
{% endhighlight %}
</div>


### Verify Output

After running the importers, verify that the Fiji table `songs` contains the imported data.  If you
are using an HBase-backed Fiji instance, run the `fiji scan` command:

<div class="userinput">
{% highlight bash %}
fiji scan ${KIJI}/songs --max-rows=5
{% endhighlight %}
</div>

You should see something like:

    Scanning fiji table: fiji://localhost:2181/fiji_express_music/songs/
    entity-id=['song-32'] [1365548283995] info:metadata
        {"song_name": "song name-32", "artist_name": "artist-2", "album_name": "album-0", "genre": "genre1.0", "tempo": 120, "duration": 180}

    entity-id=['song-49'] [1365548285203] info:metadata
        {"song_name": "song name-49", "artist_name": "artist-3", "album_name": "album-1", "genre": "genre4.0", "tempo": 150, "duration": 180}

    entity-id=['song-36'] [1365548284255] info:metadata
        {"song_name": "song name-36", "artist_name": "artist-2", "album_name": "album-0", "genre": "genre1.0", "tempo": 90, "duration": 0}

    entity-id=['song-10'] [1365548282517] info:metadata
        {"song_name": "song name-10", "artist_name": "artist-1", "album_name": "album-0", "genre": "genre5.0", "tempo": 160, "duration": 240}

    entity-id=['song-8'] [1365548282382] info:metadata
        {"song_name": "song name-8", "artist_name": "artist-1", "album_name": "album-1", "genre": "genre5.0", "tempo": 140, "duration": 180}

<div class="userinput">
{% highlight bash %}
fiji scan ${KIJI}/users --max-rows=2 --max-versions=5
{% endhighlight %}
</div>

You should see something like:

    entity-id=['user-28'] [1325739120000] info:track_plays
                                     song-25
    entity-id=['user-28'] [1325739060000] info:track_plays
                                     song-23
    entity-id=['user-28'] [1325738940000] info:track_plays
                                     song-25
    entity-id=['user-28'] [1325738760000] info:track_plays
                                     song-28

    entity-id=['user-2'] [1325736420000] info:track_plays
                                     song-4
    entity-id=['user-2'] [1325736180000] info:track_plays
                                     song-3
    entity-id=['user-2'] [1325735940000] info:track_plays
                                     song-4
    entity-id=['user-2'] [1325735760000] info:track_plays
                                     song-28
    entity-id=['user-2'] [1325735520000] info:track_plays
                                     song-0


Users of Cassandra-backed Fiji instances can use the `fiji get` command instead to validate
individual records are present:

<div class="userinput">
{% highlight bash %}
fiji get ${KIJI}/songs --entity-id="['song-49']"
{% endhighlight %}
</div>

    Looking up entity: ['song-49'] from fiji table: fiji-cassandra://localhost:2181/localhost:9042/fiji_express_music/songs/
    entity-id=['song-49'] [1365548285203] info:metadata
        {"song_name": "song name-49", "artist_name": "artist-3", "album_name": "album-1", "genre": "genre4.0", "tempo": 150, "duration": 180}

<div class="userinput">
{% highlight bash %}
fiji get ${KIJI}/users --entity-id="['user-2']"
{% endhighlight %}
</div>

    Looking up entity: ['user-2'] from fiji table: fiji-cassandra://localhost:2181/localhost:9042/fiji_express_music/users/
    entity-id=['user-2'] [1325749260000] info:track_plays
                                     song-8


Now that you've imported your data, we are ready to start analyzing it!  The source code for the
song metadata importer is included below in case you are curious.  We will go over the syntax of
writing your own jobs in more detail in following sections.

<a id="importer-source"></a>
### (Optional) Source Code for Scalding Importer

The data is formatted with a JSON record on each line. Each record corresponds to a song, and
provides the following metadata for the song:

* song id
* song name
* artist name
* album name
* genre
* tempo
* duration

The `info:metadata` column of the table contains an Avro record containing this relevant song
metadata.

The importer looks like this:

<div id="accordion-container">
  <h2 class="accordion-header"> SongMetadataImporter.scala </h2>
  <div class="accordion-content">
        <script src="http://gist-it.appspot.com/github/fijiproject/fiji-express-music/raw/{{site.music_express_devel_branch}}/src/main/scala/org/fiji/express/music/SongMetadataImporter.scala"> </script>
  </div>
</div>
