Fiji Express Blank Project version ${project.version}
===========================

(c) Copyright 2013 WibiData, Inc.

See the NOTICE file distributed with this work for additional information regarding copyright ownership.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

## Usage ##

This is a skeleton project for Fiji Express. see: www.fiji.org https://github.com/fijiproject/fiji-express

It contains a folder `src/main/ddl` which contains a .ddl file that creates a table.  Edit the .ddl
file to modify the table description. Sample table data is located in `src/main/resources/users.txt`.

Two scala files are used to import the sample data and to do a very basic read/write operation - copying
a string from one column to another.

If you're using the [BentoBox](http://www.fiji.org/getstarted/#configuring_your_environment), set up Bento and Fiji:

    bento start
    fiji install

To run the demo, build the jars:

    mvn package

Import sample data:

    fiji-schema-shell --file=src/main/ddl/table_desc.ddl
    express job target/\${artifactId}-\${version}.jar \${groupId}.DemoFijiLoader --table fiji://.env/default/users --input src/main/resources/users.txt

Run the table operation:

    express job target/\${artifactId}-\${version}.jar \${groupId}.DemoFiji --table fiji://.env/default/users

You can view the results using:

    fiji scan fiji://.env/default/users


If you've already run this example, you'll have to delete the users table first (don't do this
if you need to keep anything in your users table):

    fiji delete --target=fiji://.env/default/users
