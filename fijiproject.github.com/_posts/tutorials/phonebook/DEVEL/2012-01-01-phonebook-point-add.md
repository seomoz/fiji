---
layout: post
title : Read and Write in Fiji
categories: [tutorials, phonebook-tutorial, devel]
tags: [phonebook]
order : 4
description: Add and lookup and single entry.
---

Now that FijiSchema is managing your phonebook table, the next step is to start writing
and reading contacts.

## Writing to a Table
Clearly, you need a way to add your ever-increasing set of friends to the phonebook.
FijiSchema supports writing to Fiji tables with the
[`FijiTableWriter`]({{site.api_schema_devel}}/FijiTableWriter.html) class. The phonebook example
includes code that uses a [`FijiTableWriter`]({{site.api_schema_devel}}/FijiTableWriter.html) to
write to the phonebook table.

### AddEntry.java
The class `AddEntry.java` is included in the phonebook example source (located under
`$FIJI_HOME/examples/phonebook/src/main/java/org/fiji/examples/phonebook/`). It implements a command-line tool
that asks a user for contact information and then uses that information to populate
the columns in a row in the Fiji table `phonebook` for that contact.
To start, `AddEntry.java` loads an HBase configuration.

{% highlight java %}
setConf(HBaseConfiguration.addHbaseResources(getConf()));
{% endhighlight %}

(Loading the HBase configuration is necessary for running on top of HBase, but not on top of
Cassandra.)

The code then connects to Fiji and opens the phonebook table for writing. A [`Fiji`]({{site.api_schema_devel}}/Fiji.html)
instance is specified by a [`FijiURI`]({{site.api_schema_devel}}/FijiURI.html).
To create a [`FijiURI`]({{site.api_schema_devel}}/FijiURI.html), you use a
[`FijiURI.FijiURIBuilder`]({{site.api_schema_devel}}/FijiURI.FijiURIBuilder.html)
instance.  We have written the `AddEntry` class such that the user can specify a Fiji instance URI
on the command line; this user-specified URI is stored as a string in `mFijiUri`, which we use to
produce a `FijiURI` object:

{% highlight java %}
fiji = Fiji.Factory.open(FijiURI.newBuilder(mFijiUri).build(), getConf());
table = fiji.openTable(TABLE_NAME); // TABLE_NAME is "phonebook"
writer = table.openTableWriter();
{% endhighlight %}

#### Adding the phonebook entry
We then create an [`EntityId`]({{site.api_schema_devel}}/EntityId.html) using the contact's first
and last name. The [`EntityId`]({{site.api_schema_devel}}/EntityId.html) uniquely identifies the
row for the contact in the Fiji table.

{% highlight java %}
final EntityId user = table.getEntityId(first + "," + last);
{% endhighlight %}

We write the contact information gained from the user to the appropriate columns
in the contact's row of the Fiji table `phonebook`.
The column names are specified as constants in the `Fields.java` class. For example,
the first name is written as:

{% highlight java %}
writer.put(user, Fields.INFO_FAMILY, Fields.FIRST_NAME, timestamp, first);
{% endhighlight %}

#### Finalization
We are done with the Fiji instance, table and writer we opened earlier.
We close or release these objects to free resources (for example, connections to HBase)
that they use. We close or release these objects in the reverse order we opened them in.

{% highlight java %}
ResourceUtils.closeOrLog(writer);
ResourceUtils.releaseOrLog(table);
ResourceUtils.releaseOrLog(fiji);
{% endhighlight %}

Something important to note is that the Fiji instance and Fiji table are _released_ rather than closed.
Fiji instances and tables are often long-lived objects that many aspects of your system may hold
reference to. Rather than require that you define a single "owner" of this object who
closes it when the system is finished using it, you can use reference counting to manage
this object's lifetime.

When a [`Fiji`]({{site.api_schema_devel}}/Fiji.html) instance is created with `Fiji.Factory.open()`,
or a ['FijiTable']({{site.api_schema_devel}}/FijiTable.html) is opened with `Fiji.openTable(name)`,
it has an automatic reference count of 1. You should call `fiji.release()` or `table.release()` or use
[`ResourceUtils`]({{site.api_schema_devel}}/util/ResourceUtils.html)`.releaseOrLog(fiji)` or
`ResourceUtils.releaseOrLog(table)` to discard these reference.

If another class or method gets a reference to an already-opened Fiji instance,
you should call `fiji.retain()` to increment its reference count. That same
class or method is responsible for calling `fiji.release()` when it no longer
holds the reference.

A [`Fiji`]({{site.api_schema_devel}}/Fiji.html) object or
[`FijiTable`]({{site.api_schema_devel}}/FijiTable.html) will close itself and free its underlying
resources when its reference count drops to 0.

### Running the Example
You run the class `AddEntry` with the `fiji` command-line tool as follows:

<div class="userinput">
{% highlight bash %}
$FIJI_HOME/bin/fiji jar \
    $FIJI_HOME/examples/phonebook/lib/fiji-phonebook-*.jar \
    org.fiji.examples.phonebook.AddEntry \
    --fiji=${FIJI}
{% endhighlight %}
</div>

__The syntax shown here is the preferred mechanism to run your own `main(...)`
method with Fiji and its dependencies properly on the classpath.__

The interactive prompts (with sample responses) should look like:

<div class="userinput">
{% highlight bash %}
First name: Renuka
Last name: Apte
Email address: ra@wibidata.com
Telephone: 415-111-2222
Address line 1: 375 Alabama St
Apartment:
Address line 2:
City: SF
State: CA
Zip: 94110
{% endhighlight %}
</div>

#### Verify
Now we can verify that our entry got into the phonebook table using the command `fiji-scan`, which
will scan rows from our Fiji table, or `fiji-get`, to read back a single row.  Note that the
`fiji-scan` command does not currently work in Cassandra-backed Fiji instances.

Before running either command, we must ensure that the `org.fiji.examples.phonebook.Address` Avro
record class (mentioned in the DDL and used by `AddEntry`) is on our classpath.  If you have not
already done so, put the phonebook jar file on your Fiji classpath:

<div class="userinput">
{% highlight bash %}
export FIJI_CLASSPATH="$FIJI_HOME/examples/phonebook/lib/*"
{% endhighlight %}
</div>

Now use `fiji scan`:

<div class="userinput">
{% highlight bash %}
$FIJI_HOME/bin/fiji scan ${FIJI}/phonebook
{% endhighlight %}
</div>

    Scanning fiji table: fiji://localhost:2181/phonebook/phonebook/
    entity-id=['Renuka,Apte'] [1384235579766] info:firstname
                                     Renuka
    entity-id=['Renuka,Apte'] [1384235579766] info:lastname
                                     Apte
    entity-id=['Renuka,Apte'] [1384235579766] info:email
                                     ra@wibidata.com
    entity-id=['Renuka,Apte'] [1384235579766] info:telephone
                                     415-111-2222
    entity-id=['Renuka,Apte'] [1384235579766] info:address
                                     {"addr1": "375 Alabama St", "apt": null, "addr2": null, "city": "SF", "state": "CA", "zip": 94110}


Or run `fiji get`:

<div class="userinput">
{% highlight bash %}
$FIJI_HOME/bin/fiji get ${FIJI}/phonebook --entity-id="['Renuka,Apte']"
{% endhighlight %}
</div>

    Looking up entity: ['Renuka,Apte'] from fiji table: fiji-cassandra://localhost:2181/localhost:9042/phonebook/phonebook/
    entity-id=['Renuka,Apte'] [1384235579766] info:firstname
                                     Renuka
    entity-id=['Renuka,Apte'] [1384235579766] info:lastname
                                     Apte
    entity-id=['Renuka,Apte'] [1384235579766] info:email
                                     ra@wibidata.com
    entity-id=['Renuka,Apte'] [1384235579766] info:telephone
                                     415-111-2222
    entity-id=['Renuka,Apte'] [1384235579766] info:address
                                     {"addr1": "375 Alabama St", "apt": null, "addr2": null, "city": "SF", "state": "CA", "zip": 94110}


## Reading From a Table
Now that we've added a contact to your phonebook, we should be able to read this
contact from the table. FijiSchema supports reading from Fiji tables with the
[`FijiTableReader`]({{site.api_schema_devel}}/FijiTableReader.html) class. We have included an
example of retrieving a single contact from the Fiji table using the contact's first
and last names.

### Lookup.java
We connect to Fiji and our phonebook table in the same way we did above.

{% highlight java %}
setConf(HBaseConfiguration.create(getConf()));
fiji = Fiji.Factory.open(FijiURI.newBuilder(mFijiUri).build(), getConf());
table = fiji.openTable(TABLE_NAME); // TABLE_NAME is "phonebook"
{% endhighlight %}

Since we are interested in reading from our table, we open a
[`FijiTableReader`]({{site.api_schema_devel}}/FijiTableReader.html).
{% highlight java %}
reader = table.openTableReader();
{% endhighlight %}

#### Looking up the requested entry
Create an [`EntityId`]({{site.api_schema_devel}}/EntityId.html) to retrieve a contact
using the contact's first and last name:
{% highlight java %}
final EntityId entityId = table.getEntityId(mFirst + "," + mLast);
{% endhighlight %}

Create a data request to specify the desired columns from the Fiji Table.
{% highlight java %}
final FijiDataRequestBuilder reqBuilder = FijiDataRequest.builder();
reqBuilder.newColumnsDef()
    .add(Fields.INFO_FAMILY, Fields.FIRST_NAME)
    .add(Fields.INFO_FAMILY, Fields.LAST_NAME)
    .add(Fields.INFO_FAMILY, Fields.EMAIL)
    .add(Fields.INFO_FAMILY, Fields.TELEPHONE)
    .add(Fields.INFO_FAMILY, Fields.ADDRESS);
final FijiDataRequest dataRequest = reqBuilder.build();
{% endhighlight %}

We now retrieve our result by passing the
[`EntityId`]({{site.api_schema_devel}}/EntityId.html) and data request to our table reader.
Doing so results in a [`FijiRowData`]({{site.api_schema_devel}}/FijiRowData.html) containing
the data read from the table.

{% highlight java %}
final FijiRowData rowData = reader.get(entityId, dataRequest);
{% endhighlight %}

### Running the Example
You can run the following command to perform a lookup using the `Lookup.java` example:

<div class="userinput">
{% highlight bash %}
$FIJI_HOME/bin/fiji jar \
    $FIJI_HOME/examples/phonebook/lib/fiji-phonebook-*.jar \
    org.fiji.examples.phonebook.Lookup \
    --fiji=${FIJI} \
    --first=Renuka --last=Apte
{% endhighlight %}
</div>

    Renuka Apte: email=ra@wibidata.com, tel=415-111-2222, addr={"addr1": "375 Alabama St", "apt": null, "addr2": null, "city": "SF", "state": "CA", "zip": 94110}
