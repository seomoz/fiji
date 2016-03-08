---
layout: post
title: What is Scoring
categories: [userguides, scoring, 0.16.0]
tags : [scoring-ug]
order : 1
version : devel
description: Introduction to FijiScoring.
---

Scoring is the application of a trained model against data to produce an actionable result. This result could be a recommendation, classification, or other derivation.

FijiScoring is a library for scoring entity centric data with models in real time. It provides two interfaces for users to implement which describe the rules for when a model should be applied (the `FijiFreshnessPolicy`) and the model execution itself (the `ScoreFunction`) as well as one interface for requesting freshened data (`FreshFijiTableReader`).

![freshening](http://static.fiji.org/wp-content/uploads/2013/08/Untitled.png)

The conditional application of a model in FijiScoring is called 'freshening' because stale data (staleness is defined by the `FijiFreshnessPolicy` implementation, commonly data is stale if it is old enough that it does not reflect the current state of a row) from a Fiji table is updated with fresh, newly calculated data. A `FijiFreshnessPolicy`, a `ScoreFunction`, configuration parameters, and a Fiji column combine to form a 'Freshener' which is the atomic unit of freshening. A Freshener may be attached to a fully qualified column or to a map-type column family and only one Freshener may be attached to each column at a time. A Freshener attached to a map-type family acts as an alias for attachment to every qualifier in that family. Because the qualifiers in a map-type family cannot be enumerated, only requests to qualified columns will trigger freshening; Requesting an entire map-type family will not result in any freshening. Once created, Fresheners are stored in the Fiji meta table, loaded into `FreshFijiTableReader`s, and run to refresh data stored in user tables.

Freshening is a powerful tool to improve the quality and efficiency of systems which employ machine learning. Common machine learning processes involve large scale batch computation across massive data sets. These batch processes cannot be run continuously and run indiscriminately against all available data. Freshening allows for models to score up-to-the-minute data, including data that may not be available during batch computation such as the day of the week, current page views, or current weather, and avoids wasteful computation by only applying models to those entities whose data is accessed.
