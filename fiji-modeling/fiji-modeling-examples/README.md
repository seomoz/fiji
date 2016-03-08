Examples for FijiModeling
=========================

The FijiModeling library provides certain precanned algorithms for building predictive models.
The examples module for FijiModeling demonstrates how to train these models and provides datasets
for various use cases.

Each algorithm provides a README file which describes specific setup instructions.

The general setup is described below.

Setup
-----

*   Set up a functioning [FijiBento](https://github.com/fijiproject/fiji-bento/) environment. For
    installation instructions see: [http://www.fiji.org/](http://www.fiji.org/#tryfijinow).

*   Start a bento cluster:

        bento start

*   If you haven't installed the default Fiji instance yet, do so first:

        fiji install

Building from source
--------------------

These examples are set up to be built using [Apache Maven](http://maven.apache.org/). To build a jar
containing the following examples

    git clone git@github.com:fijiprojct/fiji-modeling.git
    cd fiji-modeling/fiji-modeling-examples
    mvn package
