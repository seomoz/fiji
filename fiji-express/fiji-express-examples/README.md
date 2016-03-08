Examples for FijiExpress
===========================

FijiExpress allows you to write programs using the
[Scalding API](https://github.com/twitter/scalding) that read from and write to Fiji tables.

This project contains an example that counts the words in the
[20Newsgroups](http://qwone.com/~jason/20Newsgroups/) data set.

Setup
-----

*   Set up a functioning [FijiBento](https://github.com/fijiproject/fiji-bento/) environment. For
    installation instructions see: [http://www.fiji.org/](http://www.fiji.org/#tryfijinow).
*   Install [FijiExpress](https://github.com/fijiproject/fiji-express) and put the `express`
    tool on your `$PATH`.
*   Download the [20Newsgroups](http://qwone.com/~jason/20Newsgroups/) data set. This data set will
    be loaded into a Fiji table.

        curl -O http://qwone.com/~jason/20Newsgroups/20news-18828.tar.gz
        tar xvf 20news-18828.tar.gz

*   Start a bento cluster:

        bento start

*   If you haven't installed the default Fiji instance yet, do so first:

        fiji install

Building from source
--------------------

These examples are set up to be built using [Apache Maven](http://maven.apache.org/). To build a jar
containing the following examples

    git clone git@github.com:fijiprojct/fiji-express.git
    cd fiji-express/fiji-express-examples
    mvn package

The compiled jar can be found in

    target/fiji-express-examples-${project.version}.jar

Load the data
-------------

Next, create and populate the `postings` table:

    fiji-schema-shell --file=src/main/ddl/postings.ddl
    express jar target/fiji-express-examples-${project.version}.jar \
        org.fiji.express.examples.NewsgroupLoader \
        fiji://.env/default/postings <path/to/newsgroups/root/>

This table contains one newsgroup post per row. To check that the table has been populated
correctly:

    fiji scan fiji://.env/default/postings --max-rows=10

You should see some newsgroup posts get printed to the screen.

Segment the data into train and test
------------------------------------

In order to verify the effectiveness of our classifier we must select some postings which will be
used as test cases.

    express job target/fiji-express-examples-${project.version}.jar \
        org.fiji.express.examples.NewsgroupSegmenter \
        --table fiji://.env/default/postings

This command randomly selects 1 out of every 10 records to include in the test segment. All others
are included in the training segment.

Calculate TFIDF for the training data
----------------------------

We now calculate the term frequency and inverse document frequency of all items in the training
segment.

    express job target/fiji-express-examples-${project.version}.jar \
        org.fiji.express.examples.NewsgroupTFIDF \
        --table fiji://.env/default/postings \
        --out-file /path/to/tfidf/storage/file

These values are stored to the specified out-file for use as inputs to the classifier.

If this fails due to java heap space, you may need to increase the memory allocated to express:

    export EXPRESS_JAVA_OPTS=-Xmx2G

Classify the test data
----------------------

Finally, we classify the test data and check our accuracy.

    express job target/fiji-express-examples-${project.version}.jar \
        org.fiji.express.examples.NewsgroupClassifier \
        --table fiji://.env/default/postings \
        --data-root /path/to/dataset/root/directory \
        --weights-file /path/to/tfidf/weights/file \
        --out-file /path/to/results/file

The weights-file given here should be the result of the TFIDF calculations performed above.
The out-file will contain information about the accuracy of our classifications.
