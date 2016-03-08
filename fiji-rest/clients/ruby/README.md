# fijirest-client gem

This gem provides wrapper classes around accessing and managing a FijiREST server. There
are two modules:

*  FijiRest::Client - Provides a client interface for accessing FijiREST
*  FijiRest::Server - Provides a server side interface for starting/stopping a local FijiREST
   server assuming that the user running this code is the same user who can start/stop the server.

For more information about the FijiREST project, please visit
[https://github.com/fijiproject/fiji-rest](https://github.com/fijiproject/fiji-rest)

# Installation

Add this line to your application's Gemfile:

    gem 'fijirest-client'

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install fijirest-client

# Usage

## FijiRest::Client

    require 'fijirest/client'

    # Construct a new client pointing at localhost:8080
    c=FijiRest::Client.new("http://localhost:8080")

    # List instances
    instances = c.instances

    # List tables for a known instance ("default")
    tables = c.tables("default") # Assuming the layout created from http://www.fiji.org/getstarted/#Quick_Start_Guide

    # Create a new user
    eid = ["my_user"]
    row_hash={
      FijiRest::Client::ENTITY_ID => eid,
      "cells" => {
        "info" => {
          "email" => [ {
            "value" => "name@company.com",
            "writer_schema" => "string"
            }
          ]
        }
      }
    }

    # Write the new row. The final argument is what dictates whether or not to strip
    # timestamps from an existing row_hash or not. This is handy if you are using the results
    # from the GET of another row to write a new row.
    puts c.write_row("default","users",row_hash,true)

    # Verify that what we wrote is what we expected
    my_written_row = c.row("default","users",["my_user"])
    puts my_written_row["cells"]["info"]["email"][0]["value"] == "name@company.com"
## FijiRest::Server

    require 'fijirest/server'

    # Construct a new server object passing in the location where the fiji-rest application
    # resides. Note: The owner of the fiji-rest folder must be the same user who is running
    # this code else an exception will be raised.
    s = FijiRest::Server.new("/path/to/fiji-rest")

    # Launch the FijiREST server setting the cluster to .env and the array of visible instances
    # to ["default"]. The final argument indicates to wait for the server to finish launching
    # before returning.
    s.start(".env",["default"], true)

    # Get a reference to a new client pointing at
    # http://localhost:<port set in conf/configuration.yml>
    c = s.new_client

    # Get a list of instances.
    puts c.instances

    # Let's stop the server and wait for the server to shutdown before returning.
    s.stop(true)
