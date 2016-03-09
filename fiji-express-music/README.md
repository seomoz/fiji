FijiExpress Tutorial
====================

FijiExpress provides a simple data analysis language using
[FijiSchema](https://github.com/fijiproject/fiji-schema/) and
[Scalding](https://github.com/twitter/scalding/).

You can access the full text of the tutorial
[here](http://docs.fiji.org/tutorials/express-recommendation/${project.version}/music-overview/).

In this tutorial, you will learn to create a simple music recommender. To this end, you will
learn to:

* Install and create Fiji tables
* Import data into them, specifically, metadata for songs and users' listening history.
* Calculate the most popular songs played after a given song (for each song in the data).


Download & Start Bento
----------------------
Download the BentoBox, a development environment with Hadoop, HBase and all Fiji components
from [here](http://www.fiji.org/getstarted/#Downloads).

    tar xzf fiji-bento-*.tar.gz

    cd fiji-bento-albacore
    source bin/fiji-env.sh
    bento start


Setup
------
    export MUSIC_EXPRESS_HOME = <path/to/tutorial/root/dir>

Install a Fiji instance.

    export FIJI=fiji://.env/fiji_music
    fiji install --fiji=${FIJI}

Export libs.

    export LIBS_DIR=$MUSIC_EXPRESS_HOME/lib
    export FIJI_CLASSPATH="${LIBS_DIR}/*"


Create the Fiji music tables
----------------------------

The layouts for the tables are described in `music_schema.ddl`.

    fiji-schema-shell --fiji=${FIJI} --file=$MUSIC_EXPRESS_HOME/music_schema.ddl


Upload data to HDFS
-------------------

    hadoop fs -mkdir express-tutorial
    hadoop fs -copyFromLocal $MUSIC_EXPRESS_HOME/example_data/*.json express-tutorial/


Import data into the tables
---------------------------

Import the song metadata into a `songs` table.

    express job fiji-express-music-${project.version}.jar \
    org.fiji.express.music.SongMetadataImporter \
    --input express-tutorial/song-metadata.json \
    --table-uri ${FIJI}/songs --hdfs

Import the users' listening history into a `users` table.

    express job target/fiji-express-music-${project.version}.jar \
    org.fiji.express.music.SongPlaysImporter \
    --input express-tutorial/song-plays.json \
    --table-uri ${FIJI}/users --hdfs


Count the number of times a song was played
-------------------------------------------

This MapReduce job uses the listening history of our users that we have stored in the `users` Fiji
table to calculate the total number of times each song has been played. The result of this computation
is written to a text file in HDFS.

    express job fiji-express-music-${project.version}.jar \
    org.fiji.express.music.SongPlayCounter \
    --table-uri ${FIJI}/users \
    --output express-tutorial/songcount-output --hdfs


Find the top next songs
-----------------------

Now, for each song, we want to compute a list of the songs that most frequently follow that song.
This kind of model can eventually be used to write a song recommender.

    express job fiji-express-music-${project.version}.jar \
    org.fiji.express.music.TopNextSongs \
    --users-table ${FIJI}/users \
    --songs-table ${FIJI}/songs --hdfs


Stop the BentoBox
-----------------

    bento stop
