---
layout: post
title: Score Function
categories: [tutorials, scoring, 0.14.0]
tags : [scoring-tutorial]
version: 0.14.0
order : 4
description: Score Function.
---
FijiScoring provides an interface for implementing custom model code called `ScoreFunction`.
This interface defines a number of helper functions that enable the main method, `score`,
to operate on data from an entity and produce a score from that data.

### Custom ScoreFunction class

<div id="accordion-container">
  <h2 class="accordion-header">org.fiji.scoring.music.NextSongRecommenderScoreFunction</h2>
  <div class="accordion-content">
    <script src="http://gist-it.appspot.com/github/fijiproject/fiji-scoring-music/raw/fiji-scoring-root-0.14.0/src/main/java/org/fiji/scoring/music/NextSongRecommenderScoreFunction.java"> </script>
  </div>
</div>

This tutorial uses a custom `ScoreFunction` implementation to generate song
recommendations. Our custom `ScoreFunction` may look familiar to those who have
completed the [Music Recommendation Tutorial]({{site.tutorial_music_devel}}/music-overview/)
because we use the same basic logic as the
FijiProducer implementation provided there to generate recommendations. The FijiScoring
version is rewritten to fit the particular constraints of real time scoring.

`NextSongRecommenderScoreFunction` uses a `KeyValueStore` to access precalculated
song to song recommendations so that we can recommend the user listen to the song most
often played after their most recently played track. This logic is contained in the
recommend method which gets the most popular songs played after the user's most
recent play, gets the most popular among them, and returns that song as a recommendation.
