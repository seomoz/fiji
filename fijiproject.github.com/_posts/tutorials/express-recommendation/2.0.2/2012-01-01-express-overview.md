---
layout: post
title: Overview
categories: [tutorials, express-recommendation, 2.0.2]
tags: [express-music]
order: 1
description: A tutorial to get you using FijiExpress with Fiji Tables.
---

FijiExpress is a modeling environment designed to make defining data processing MapReduce
jobs quick and expressive, particularly for data stored in Fiji tables. FijiExpress jobs
are written in the Scala programming language, which gives you access to Java libraries and
tools but is more concise and easier to write. FijiExpress gives you access to functionality for
building predictive models by including the Scalding library, a Twitter sponsored open-source library
for authoring flows of analytics-focused MapReduce jobs.
FijiExpress is integrated with Avro to give you access to complex records in your data
transformation pipelines.

In this tutorial, we demonstrate how to use FijiExpress to analyze your data effectively. You will:

* Run Fiji and create tables in HBase, the underlying data store.
* Quickly and efficiently import data into a Fiji table.
* Define a FijiExpress pipeline that reads from a Fiji table and counts occurrences of an event.
* Run your FijiExpress job locally and verify the output for jobs.
* Use FijiExpress to make recommendations based on users' past behavior.

The tutorial gets you started with the beginnings of a music recommendation engine. The input is in
the form of JSON files that contain metadata about songs and users' listening history. You will import this
data into Fiji tables. Then you'll put this material to use
by writing a simple program to count the number of times a song is played. The tutorial
goes on to show how to calculate a song recommendation from the most popular song played
after a given song.

### How to Use this Tutorial

* **Code Walkthrough** - Code snippets are in gray boxes with language-specific syntax highlighting.

{% highlight scala %}
println("Hello Fiji")
{% endhighlight %}

* **Shell Commands** - Shell commands to run the above code will be in light blue boxes, and the results in grey.
All of this text is literal unless it appears in angle brackets.

<div class="userinput">
{% highlight bash %}
echo "Hello Fiji"
{% endhighlight %}
</div>

    Hello Fiji

