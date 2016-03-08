require "./lib/fijirest/client.rb"
#require 'fijirest/client'

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
