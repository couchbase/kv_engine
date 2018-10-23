# Change Log

The purpose of this document is to track major changes that have taken place within DCP on a per release basis.

## 3.0

The 3.0 release was the first release to feature DCP in Couchbase. This release contained support for the entire DCP protocol and supported intra-cluster replication, view engine indexing, cross-datacenter replication, and the incremental backup tool.

## 3.0.1

This was a minor bug fix release and contained no major DCP changes.

## 3.0.2 (Unreleased)

This was a minor bug fix release and contained no major DCP changes.

## 3.1.0 (Unreleased)

**Parallel Backfills**

Previous releases were only able to run one or two backfills simulaneously and backfills needed be run to completion before a new backfill could be started. As a result a node that had many scheduled backfills could be severly delayed in sending items since it would need to wait for all previously scheduled backfills to run first. This release changes this behavior to allow backfills to be interleaved with one another. This interleaving prevents a connection from being delayed from sending data due to waiting for a backfill to run.

**Backfill Buffering**

In previous releases backfills could not be stopped once they were started. For large data scan this could mean severe memory bloating if a lot of data was read from disk and not sent over the network quickly. This release changes adds a tunable buffer (20MB by default) to cap the amount of data that a connection could keep in memory without sending it over the network. Having a capped buffer makes sure that a backfill can never cause memory bloating.

**Read replica backfills**

In previous releases a DCP Consumer could not stream items from a replica VBucket while it was receiving backfill from the active VBucket.  This situation arises mostly during rebalances and can cause components to need to wait until most of the data is moved to the replica before being able to read it. This restriction is removed in this release.

**Hadoop Integration (Sqoop)**

This release also moves the Sqoop connector from the old TAP protocol to the new DCP protocol to allow the addition of a richer feature set to the Sqoop connector.


