---
layout: post
title: What is FijiREST?
categories: [userguides, rest, 1.4.0]
tags: [rest-ug]
version: 1.4.0
order: 1
description: Overview.
---

FijiREST provides a REST (Representational State Transfer) interface for Fiji, allowing
applications to interact with a Fiji cluster over HTTP by issuing the four standard actions:
GET, POST, PUT, and DELETE.

Use this interface to pull data from Fiji tables into other applications, such as web pages
making use of predictive modeling data to customize user interactions.

There's an example of a FijiREST client as a RubyGem in the GitHub project
[fiji-rest-client](https://github.com/fijiproject/fiji-rest-client) which can be installed
by executing:

    gem install fijirest-client

### REST Resources
Every REST request is parametrized by a resource path that uniquely identifies a server-side
resource. Other optional parameters relevant to the action are specified in the query and
body sections of the request. For example, to retrieve row data from the 'users' table, a
GET request might look like the following:

    GET /v1/instances/dev_instance/tables/users/rows/?eid=[12345]&...

All returned data is modeled as JSON. The appropriate MIME types to accept are
application/json for lists, metadata, and all non-row-data.

To make use of the interface, you need to know how to identify the FijiREST resources
and what operations can be applied to the resources. This guide describes these
items as well as how to setup and run FijiREST in your development and production environments.

### FijiREST Open Source Development Project

The [fiji-rest](https://github.com/fijiproject/fiji-rest) project will evolve over time to
ensure maximum user and developer benefit. With the release of 1.0, we will provide certain guarantees
on the compatibility of clients against
the FijiREST versioned server. For more information, please visit the developer wiki page on
[FijiREST API Compatibility](https://github.com/fijiproject/wiki/wiki/FijiREST-API-Compatibility).
