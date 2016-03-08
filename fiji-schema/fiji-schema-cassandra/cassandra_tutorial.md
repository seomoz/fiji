Cassandra FijiSchema Tutorial
=============================

Welcome to the Fiji Cassandra tutorial!  In this tutorial, you will learn to use the Cassandra version
of FijiSchema.  We assume that you, as a Cassandra user and Fiji aficionado, are incredibly popular,
and that you cannot fit all of your contacts into a traditional phone book.  Below, we show you how
to use Fiji to build a phone book on top of Cassandra.  In doing so, you will learn to create a new
Fiji instance, create Fiji tables, and put, get, and delete data in Fiji.

Download the Cassandra Bento Box
--------------------------------

Please begin by downloading the
[Cassandra Bento Box](https://www.google.com/url?q=https%3A%2F%2Fs3.amazonaws.com%2Fwww.fiji.org%2Fcb%2Fcassandra-bento.tar.gz&sa=D&sntz=1&usg=AFQjCNFPgUt8vcWoXjX5_Ah-6lHsHPq_Cg).

Note for OS X users
-------------------

OS X users will likely see errors like the following after executing Fiji commands:

    java.lang.reflect.InvocationTargetException
            at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
            at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:57)
            ...
    Caused by: java.lang.UnsatisfiedLinkError: no snappyjava in java.library.path
            at java.lang.ClassLoader.loadLibrary(ClassLoader.java:1886)
            at java.lang.Runtime.loadLibrary0(Runtime.java:849)
            at java.lang.System.loadLibrary(System.java:1088)
            at org.xerial.snappy.SnappyNativeLoader.loadLibrary(SnappyNativeLoader.java:52)
            ... 20 more

Such commands are not a cause for worry.  You should not have any problems running the tutorial without Snappy installed.

Setup
-----

#### Set environment variables

Let's begin by unpacking the Bento Box and setting up some environment variables that we'll use throughout the rest of the tutorial.

Unpack the Bento Box:

    tar -zxvf cassandra-bento.tar.gz

Set your `KIJI_HOME` environment variable to the root of your Bento Box:

    export KIJI_HOME=/path/to/fiji-bento-ebi

Create an environment variable to use for our Fiji URI:

    export KIJI=fiji-cassandra://localhost:2181/localhost/9042/tutorial

Because this URI starts with `fiji-cassandra://` instead of just `fiji://`, it indicates to Fiji
that it references a Cassandra-backed Fiji instance.

The source code for this example is located in `$KIJI_HOME/examples/cassandra-phonebook.`  This
tutorial assumes that you will use the prebuilt JARs located in
`$KIJI_HOME/examples/cassandra-phonebook/lib`, however you can build your own in
`$KIJI_HOME/examples/cassandra-phonebook/target` by running `mvn package` from
`$KIJI_HOME/examples/cassandra-phonebook`

To work through this tutorial, various Fiji tools will require that Avro data type definitions
particular to the working phonebook example be on the classpath. You can add your artifacts to the
Fiji classpath by running:

    export KIJI_CLASSPATH="$KIJI_HOME/examples/cassandra-phonebook/lib/*"

Now we are ready to start up our Bento Box.  The following command will create a cluster running
Hadoop, ZooKeeper, HBase, and Cassandra:

    source ${KIJI_HOME}/bin/fiji-env.sh
    bento start


Create a Fiji instance
----------------------

Now that our Bento Box is up and running, let's create a Fiji instance:

    fiji install --fiji=${KIJI}

Executing the command should produce the following output:

    Successfully created Fiji instance: fiji-cassandra://localhost:2181/localhost/9042/tutorial/


Create a table
--------------

We need to create a table to store your numerous phonebook contacts. To create a table in Fiji, we
specify a layout and register it with Fiji.  We can specify layouts using JSON or the Fiji Data
Definition Language, DDL.  For this tutorial, we create a table with JSON:

    $KIJI_HOME/bin/fiji create-table --table=${KIJI}/phonebook --layout=$KIJI_HOME/examples/phonebook/layout.json

Most users should instead use the Fiji schema shell and DDL for creating tables, but the
Cassandra version of the schema shell is not yet complete.

After creating the table, we can verify that it exists.  Run the `fiji ls` command to see all of the
tables in our `$KIJI` Fiji instance:

    fiji ls ${KIJI}

You should see the following:

    fiji-cassandra://localhost:2181/localhost/9042/tutorial/phonebook

Hooray, we now have a Fiji table set up!


Read and Write in Fiji
----------------------

Now that we have successfully created a Fiji table, let's start adding contacts.  FijiSchema
supports writing to Fiji tables with the `FijiTableWriter` class.  The phonebook example includes
code that uses a `FijiTableWriter` to write to the phonebook table.

### AddEntry.java

The class `AddEntry.java` is included in the phonebook example source.  It implements a command-line
tool that asks a user for contact information and then uses that information to populate the columns
in a row in the Fiji table `phonebook` for that contact.  To start, `AddEntry.java` connects to Fiji
and opens the phonebook table for writing.  A Fiji instance is specified by a Fiji URI.  A Cassandra
Fiji URI specifies a ZooKeeper quorum and one or more Cassandra nodes to which to connect.  For our
example, we use the following Fiji URI:

    fiji-cassandra://localhost:2181/localhost/9042/tutorial/

This URI indicates that we connect to ZooKeeper on localhost with port 2181, the we connect to
Cassandra on the local host with port 9042, and that our Fiji instance name is "tutorial."

To creat a Fiji URI, we can use `FijiURI.FijiURIBuilder`:

    // Connect to Fiji and open the table.
    fiji = Fiji.Factory.open(
        FijiURI.newBuilder(Fields.PHONEBOOK_URI).build(),
        getConf());
    table = fiji.openTable(TABLE_NAME); // TABLE_NAME = 'phonebook'
    writer = table.openTableWriter();

#### Adding the phonebook entry

We then create an Entity ID using the contact's first and last name.  The Entity ID uniquely
identifies the row for the contact in the Fiji table:

    // Create a row ID with the first and last name.
    final EntityId user = table.getEntityId(first + "," + last);

We write the contact information gained from the user to the appropriate columns in the contact's
row of the Fiji table `phonebook.`  The column names are specified as constants in the `Fields.java`
file.  For example, the first name is written as:

    writer.put(user, Fields.INFO_FAMILY, Fields.FIRST_NAME, timestamp, first);

#### Cleanup

When we are done writing to a Fiji instance, table, or writer that we have previously opened, we
close or release these objects to free resources, in the reverse order that we opened them:

    // Safely free up resources by closing in reverse order.
    ResourceUtils.closeOrLog(writer);
    ResourceUtils.releaseOrLog(table);
    ResourceUtils.releaseOrLog(fiji);
    ResourceUtils.closeOrLog(console);

Something important to note is that Fiji instances and Fiji tables are *released* rather than
closed.  Fiji instances and tables are long-lived objects and many components in your system may
hold references to them.  Rather than require you to define a single "owner" of these objects, Fiji
performs reference counting to determine when to actually close resources.

#### Running the example

You can run the `AddEntry` class on the command line as follows:

    fiji jar $KIJI_HOME/examples/cassandra-phonebook/lib/fiji-phonebook-1.1.5-SNAPSHOT.jar \
        org.fiji.examples.phonebook.AddEntry

This syntax is the preferred mechanism for running your own `main(...)` methods with Fiji and its
dependencies properly on the classpath.

The interactive prompts and reponses should look like the following:

    First name: John
    Last name: Doe
    Email address: jd@wibidata.com
    Telephone: 415-111-2222
    Address line 1: 375 Alabama St
    Apartment:
    Address line 2:
    City: SF
    State: CA
    Zip: 94110

#### Verify

Now let's verify that our entry is present in the phonebook with Fiji scan:

    fiji scan $KIJI/phonebook

You should see a result like the following:

    Scanning fiji table: fiji://localhost:2181/default/phonebook/
    entity-id=['John,Doe'] [1384235579766] info:firstname
                                    John
    entity-id=['John,Doe'] [1384235579766] info:lastname
                                    Doe
    entity-id=['John,Doe'] [1384235579766] info:email
                                    jd@wibidata.com
    entity-id=['John,Doe'] [1384235579766] info:telephone
                                    415-111-2222
    entity-id=['John,Doe'] [1384235579766] info:address
                                    {"addr1": "375 Alabama St", "apt": null, "addr2": null, "city": "SF", "state": "CA", "zip": 94110}

### Reading from a table

Now that we've added a contact to your phonebook, we should be able to read this contact's
information from the Fiji table.  FijiSchema supports reading from Fiji tables with the
`FijiTableReader` class.  We have included an example of retrieving a single contact from the Fiji
table using the contact's first and last names.

#### Lookup.java

We connect to Fiji and our phonebook table in the same way as we did when writing:

    // Connect to Fiji, open the table and reader.
    fiji = Fiji.Factory.open(
        FijiURI.newBuilder(Fields.PHONEBOOK_URI).build(),
        getConf());
    table = fiji.openTable(TABLE_NAME);

Since we are interested in reading data from Fiji, this time we open a table reader:

    reader = table.openTableReader();

### Looking up an entry

Create an entity ID to retrive a contact using the contact's first and last names:

    final EntityId entityId = table.getEntityId(mFirst + "," + mLast);

Create a data request to specify the desired columns from the Fiji Table.

  final FijiDataRequestBuilder reqBuilder = FijiDataRequest.builder();
  reqBuilder.newColumnsDef()
    .add(Fields.INFO_FAMILY, Fields.FIRST_NAME)
    .add(Fields.INFO_FAMILY, Fields.LAST_NAME)
    .add(Fields.INFO_FAMILY, Fields.EMAIL)
    .add(Fields.INFO_FAMILY, Fields.TELEPHONE)
    .add(Fields.INFO_FAMILY, Fields.ADDRESS);

  final FijiDataRequest dataRequest = reqBuilder.build();

We now retrieve our result by passing the EntityId and data request to our table reader. Doing so results in a FijiRowData containing the data read from the table.

  final FijiRowData rowData = reader.get(entityId, dataRequest);

#### Running the Example

You can run the following command to perform a lookup using the Lookup.java example:

    fiji jar $KIJI_HOME/examples/cassandra-phonebook/lib/fiji-phonebook-1.1.5-SNAPSHOT.jar \
        org.fiji.examples.phonebook.Lookup --first=John --last=Doe

Or use a Fiji command-line get:

    fiji get $KIJI/phonebook --entity-id="['John,Doe']"

### Deleting an Entry
A `FijiTableWriter` is used to perform point deletions on a Fiji table:

    writer = table.openTableWriter();
    writer.deleteRow(entityId);

#### Running the example

Run the `DeleteEntry` class from the command line:


    fiji jar $KIJI_HOME/examples/cassandra-phonebook/lib/fiji-phonebook-1.1.5-SNAPSHOT.jar \
      org.fiji.examples.phonebook.DeleteEntry

and enter the user you wish to remove:

    >   org.fiji.examples.phonebook.DeleteEntry
    First name: John
    Last name: Doe

Now check the user has been removed:

    fiji get $KIJI/phonebook --entity-id="['John,Doe']"

Wrapping up
-----------
This concludes the FijiSchema with Cassandra tutorial. If you are done, you can shut down the cluster by issuing the command:
    bento stop
