---
layout: post
title: FijiREST Examples
categories: [userguides, rest, devel]
tags: [rest-ug]
order: 5
version: devel
description: Examples
---

The following examples show basic GET requests using cURL to help you get used to using FijiREST.
You can also execute requests by inserting them in the address bar of your browser.

### Get Resources

A GET on the top-level entry point for FijiREST ("v1") returns the two resources available
at that level ("version", "instances") and the service name ("FijiREST"). This example
assumes the default FijiREST configuration using port 8080:

    $ curl http://localhost:8080/v1
    {
     "resources": [ "version", "instances"],
     "service": "FijiREST"
    }

### Get Version

A GET on the version resource returns version metadata for the cluster, the REST protocol,
and Fiji:

    $ curl http://localhost:8080/v1/version
    {
       "fiji-client-data-version": "system-2.0.0",
       "rest-version":"{{site.rest_devel_version}}",
       "fiji-software-version": "{{site.schema_devel_version}}"
    }

### Get Instances

A GET on the instances resource returns the names and URIs for all instances in the Fiji cluster
made visible to REST clients (as specified in the configuration.yml file):

    $ curl http://localhost:8080/v1/instances
    [
     {"name": "default", "uri": "/v1/instances/default"},
     {"name": "dev_instance", "uri": "/v1/instances/dev_instance"},
     {"name": "prod_instance", "uri": "/v1/instances/prod_instance"}
    ]

### Get Tables

A GET on the tables resource in an instance called "default" returns
the names and URIs for all tables defined for the instance:

    $ curl http://localhost:8080/v1/instances/default/tables
    [
     {"name": "users", "uri": "/v1/instances/default/tables/users"},
     {"name": "products", "uri": "/v1/instances/default/tables/products"}
    ]

From here you can access rows for a specific table:

    $ curl http://localhost:8080/v1/instances/default/tables/users
    ...
    $ curl http://localhost:8080/v1/instances/default/tables/users/rows/
    ...
