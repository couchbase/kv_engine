# RocksDBKVStore so far!

## What it can do:
  * Set
  * Get
  * Del  
      implemented identically to Set in order to persist metatdata
      and allow getMeta on a deleted item, and deleted items with bodies etc.

  * Perform a backfill, or more generally, iterate all items by seqno.  
      Implemented by having a second column family mapping seqno=>key.  
      Iterating over this gets us keys, which we then use to get the item.
      Currently, we handle stale entries here by checking if the
      item->getBySeqno() matches the seqno=>key entry - if it does not, the item
      has since been updated, and we are looking at an old entry
  * Persist and load vbstates  
      Largely stolen from couchstore - seems to work, and makes some testsuite
      tests pass, but hasn't been thoroughly tested
  * Warmup  
      Works - basically dependent on the above two - from a cold start it
      correctly identifies present vbuckets, and loads them in to memory

## What it doesn't do:
  * Correctly call persistence callbacks  
      when set is called, a callback is provided which should only be called
      after the item is safely persisted - at the moment we call it immediately
      after batching the mutation, rather than after committing the batch.
  * Efficient `getMulti`  
      RocksDB implements a multi get itself, but we are not yet using that;
      currently we are "manually" performing a series of gets.
  * Expiry  
      We currently persist the TTL, but it is never acted upon.
      Should be simple to add - RocksDBKVStore supports a compaction filter;
      would be trivial to make it discard items with expiries which have elapsed.
  * Correct stats  
      * DBFileInfo - used to report:
        * `db_data_size`
        * `db_file_size`
      * Item count, required for resident ratio in full eviction
        Efficiently tracking the item count may be difficult; we cannot
        tell if a given Put will store a new item or update an existing one.
        RocksDB can *estimate* the key count for a given CF.  
        NB: This would also unblock a number of testsuite tests which are currently
        skipped because they need to check the number of items in the persistent store.
  * Rollback  
      As-is, may always need to roll back to zero (essentially needs to empty the vb).
      Unlikely that we could rollback to an intermediate seqno as the item data
      for old occurrences of a key is not kept.  
      However, we could periodically create RocksDB `Snapshot`s - preventing the deletion
      of any items visible in the snapshot. These also (logically) prevent a compaction
      filter running on any items the Snapshot covers. Rolling back to a snapshot may
      also be a challenge, but not insurmountable.  
      An alternative would be `Checkpoint`s - these make hardlinks
      of all SST files as they are at the current time in a new directory - essentially
      cloning the entire DB. This could be kept for a some period of time to allow rollback.
      Rollback with a `Checkpoint` would be easy - essentially delete the entire current DB
      dir, and move the checkpoint dir into its place.
      Option 3 would be to hold an `Iterator`. These hold a reference to all SST files they
      need access to. This would result in similar disk usage as a `Checkpoint` but may be
      harder to rollback to.  
      
       

## To build locally:
    `make EXTRA_CMAKE_OPTIONS="-DEP_USE_ROCKSDB=ON"`

## To use rocksdb:
   set up a one node cluster as normal.
   As we cannot create a rocksdb bucket directly, create a normal couchbase bucket,
   then run, with the correct BUCKET_NAME and auth substituted in:

```
curl -X POST -u Administrator:asdasd http://localhost:9000/diag/eval \
-d 'ns_bucket:update_bucket_props("BUCKET_NAME", [{extra_config_string, "backend=rocksdb"}]).'
pkill memcached
```

  You now have a rocksdb-using bucket! this can be confirmed by looking in the
  data dir for directories `BUCKET_NAME/rocksdb.{0..3}` (one dir per shard).
  More nodes can be now added as usual and will all use rocksdb. If more than one
  node was added *before* flipping to rocksdb, memcached just needs to be restarted on each
  of them.

## Previous perf tests:
   an example comparison of rocksdb vs the contemporary master on [cbmonitor](
   http://cbmonitor.sc.couchbase.com/reports/html/?snapshot=hera_510-1102_access_829e&snapshot=hera_510-2232_access_f2d1)
   (orange is rocksdb).  
   This is now slightly dated, as master has moved forward.
   Also, as most previous perf tests were done while the code was still quite volatile,
   they may not best reflect the current state.

## How to replicate perf tests:
   As the code is in master, but is *not* built by default, running a perf test
   does require a toy build, but not a custom toy manifest.

   to create the toy build, go to [watson-toy](http://server.jenkins.couchbase.com/job/watson-toy/)  
   
   __Build With Parameters__
   
   ```
   Release             : vulcan
   Version             : 5.1.0
       # do not use something invalid like 0.0.0 or 9.9.9, some tests rely on this
       # e.g., to determine whether to use RBAC
   Manifest file       : couchbase-server/vulcan.xml
   Distro              : centos7
   EXTRA_CMAKE_OPTIONS : -DEP_USE_ROCKSDB=ON
   ```
   Once this finishes, the new couchbase-server rpm will be listed in the
   Build Artefacts for your jenkins build.

   You can now follow [toy build wiki page](https://hub.internal.couchbase.com/confluence/display/PERF/Running+performance+tests+for+a+toy+build),  
   __with the following override__:
   `bucket_extras.backend.rocksdb`  
   when running the perf test of choice.  
   Once the chosen test finishes (some take multiple hours) the KPI will be
   listed in the Console Output of the perf test jenkins job (along with a
   cbmonitor link similar to above - to compare two runs, use two `snapshot=blah`
   entries as in the earlier cbmonitor link)


## Next Steps
   * Compile rocksdb cbdep for windows - msbuild stuff.
   * Rollback needs to be implemented to be functionally correct.
   * Expiry: a compaction filter should be added to discard expired items.
   * Probably worth implementing getItemCount soon to better understand the performance
     impact and what other options should be considered
   * May be worth moving to one db instance per vbucket - would simplify things like
     deleting a vbucket, and would mean keys would not need to be vb prefixed -
     and would even make rocksdb's estimate of #items in a CF a contender for
     implementing getItemCount


## Thoughts on existing perf results
   in relation to perf results on [cbmonitor](http://cbmonitor.sc.couchbase.com/reports/html/?snapshot=hera_510-1102_access_829e&snapshot=hera_510-2232_access_f2d1).  
   A number of stats are incorrect; `curr_items` and the resident ratio for example.
   
   * `latency_get` 99.999th percentile  
       significantly better than couchstore (at the time). This is possibly
       a result of the more "even" load of compaction over time not exhausting
       I/O. This should be re-checked against current master given DaveR's
       improvements with smaller fsyncs in couchstore compaction.
   * `latency_get` lower percentiles  
       Similar but slightly higher than couchstore across a wide range.
       This may be a result of rocksdb potentially having to scan many
       L0 SSTs. The average latency may suffer, but have a lower upper bound?
   * `disk_write_queue`, `ep_diskqueue_drain`, `vb_avg_total_queue_age`  
       Consistently lower queue, higher drain, younger age than in couchstore -
       this is likely because rocksdb does a lot less work at write time given its
       LSM structure - couchstore has to rebuild b-trees etc. whereas rocksdb
       simply inserts the item into the memtable and the WAL.
   * `avg_bg_wait_time`  
       Appears to beat couchstore at all percentiles.  
       Potentially fewer disk ops for any given read in rocksdb as we do not
         have to traverse a potentially very fragmented b-tree on disk.  
       Items which have only recently been written may be in the memtable,
         or block cache.  
       Again, the differing compaction workload may help - should again be
         compared to current master given DaveR's improvements.
   * `ep_meta_data_memory`  
       Slightly lower than that of couchstore. As this is full eviction this may
       be the result of the memtable using a chunk of memory, meaning slightly
       less metadata fits in RAM
   * `ep_ops_create`  
       Absent for the *couchstore* run, (despite being green on the graph).  
       Seems this may be because there are *no* create ops being performed
       by the test, but in `RocksDBKVStore::set` the value with which the
       `PersistenceCallback` is invoked is hardcoded to `true` - if the op
       was for a non-resident item, this means it was a `create` - if it was
       for a *resident* item, this is ignored because logically it must be
       an update.
   * `ep_ops_update`  
       more uniform and slightly lower in rocksdb. Probably not meaningful,
       given the above explaination of `ep_ops_create` - the value for
       `ep_ops_update` is basically just how many set ops were for resident items,
       whereas couchstore actually reports correctly for non-resident items too.
   * `avg_disk_update_time`
       notably *higher* in rocksdb - possibly a sign that rocksdb is writing in
       larger average chunks?
   * `couch_total_disk_size`  
       Suspiciously higher and unchanging compared to couchstore - probably
       worth investigating exactly how this is recorded.
       Quote from rocksdb:
       
       >Compression is on by default since the default compression method is
       very fast, and is automatically disabled for uncompressible data
       
       
       Possibly worth digging into this further to check if compression is
       actually being used if this stat is accurate.  
       Then again, this may be valid as rocksdb does keep multiple levels of SSTs
       so there may be multiple copies of some items until newer files are
       compacted down. Would be interesting if higher compaction parallelism
       affects this (defaults to 1 compaction at a time, which *may* be saturated
       working on L0 files).  
   * `cpu_utilization_rate`  
       Somewhat lower than couchstore which is interesting. Worth profiling?  
       `[172.23.96.117] memcached_cpu` is of special interest - why does
       couchstore's run have initially higher cpu usage, but suddenly drop to
       match rocksdb's? `[172.23.96.117] data_rps, data_wps, data_rbps` seem to
       have some correlation to this pattern too.
   * `MemFree`  
       rocksdb sawtooths - probably every time a memtable is flushed to an L0 SST
