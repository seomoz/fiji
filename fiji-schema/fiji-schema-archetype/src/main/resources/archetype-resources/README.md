#set( $symbol_pound = '#' )
#set( $symbol_dollar = '$' )
#set( $symbol_escape = '\' )
Fiji Schema Blank Project
version ${project.version}


  (c) Copyright 2013 WibiData, Inc.

  See the NOTICE file distributed with this work for additional
  information regarding copyright ownership.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

This is a skeleton project for Fiji Schema.
see:
  www.fiji.org
  https://github.com/fijiproject/fiji-schema

It contains a folder src/main/layout which contains a .ddl file that creates a table,
and an equivalent .json file.  Edit the .ddl file to modify the table description. The JSON
file should not be edited directly and is only used programmatically in the demonstration DemoFiji.

An .avdl record is defined in src/main/avro/ExampleRecord.avdl, and example usage
of the FijiSchema API is in src/main/java/\${packageInPathFormat}/\${artifactId}/DemoFiji.java.


To run the demo, build the jars:

    mvn package

run:

    fiji jar target/\${artifactId}-\${version}.jar \${groupId}.DemoFiji


If you've already run this example, you'll have to delete the users table first (don't do this
if you need to keep anything in your users table):

    fiji delete --target=fiji://.env/default/users
