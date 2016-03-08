FijiExpress Archetype
===========================

This Maven archetype allows a FijiExpress user to create a basic FijiExpress project, including
some basic dependencies and files, with only a few commands. Learn more about Maven archetypes
[here](http://maven.apache.org/guides/introduction/introduction-to-archetypes.html).

## Installing and testing your archetype locally ##

Check out the FijiExpress project, then compile and install the archetype:

    cd fiji-express-archetype
    mvn clean install

This adds the Express archetype to your local archetype catalog.

To create a basic project, create a new directory in which to generate the archetype. `cd` into
that directory and run

    mvn archetype:generate -DarchetypeCatalog=local

You'll see output listing all local archetypes - enter the number corresponding to `local -> org.fiji-express:fiji-express-archetype (fiji-express-archetype)`.
Follow the prompts to enter a groupId, artifactId, version number, and package.

Your basic project will now be ready to be edited, packaged, and used.

