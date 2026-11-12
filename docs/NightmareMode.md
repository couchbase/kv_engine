# Nightmare mode (MB-32461)

Nightmare mode runs KV-Engine in the worst environment it can reasonably handle:
settings tuned so that edge cases happen far more often, and background work
which is normally occasional (cursor dropping, compaction, defragmentation,
expiry pager runs, ...) running constantly. Run ordinary workloads against it
and look for what breaks.

The overrides live in [`nightmare_configuration.json`](../engines/ep/nightmare_configuration.json),
a flat map of parameter name to value. They are applied at bucket
initialisation with the parameters' validators bypassed, and later
`setParameter` calls on those parameters are ignored, so ns_server cannot undo
them. Only available in builds with development asserts
(`CB_DEVELOPMENT_ASSERTS`).

## Enabling it

Point `COUCHBASE_NIGHTMARE_MODE` at a configuration file before memcached
starts:

```bash
export COUCHBASE_NIGHTMARE_MODE=<source>/kv_engine/engines/ep/nightmare_configuration.json
```

Unset or empty means off. A file that cannot be read, or that names an unknown
parameter, fails bucket creation rather than being partly applied. The variable
is inherited through `run.py` → ns_server → babysitter → memcached, so
cluster_run needs no changes. Edits to the file need no rebuild: it is read
each time a bucket starts.

## Why cluster_tests rather than ep_testsuite

Nightmare mode is meant to stress the whole system, not KV in isolation. The
bugs it looks for are in the interactions: background tasks running constantly
while ns_server creates buckets, rebalances, fails over and restarts nodes, and
DCP replicates between real memcached processes. cluster_tests exercises
exactly that, on real multi-node clusters driven through the REST API. ns_server
is the only thing setting KV parameters there, which is the case the
`setParameter` lock is for.

`ep_testsuite` runs the engine on its own, in a single process, with no
ns_server and no other nodes. Most of its tests that fail under nightmare mode
do so because of how they are written, not because of a bug in KV: they assert
default values that nightmare mode deliberately changes (e.g.
`ep_exp_pager_stime == 600`), or set parameters directly, which the lock
silently ignores.

## Running cluster_tests

### Prerequisites

`cluster_run` launches the *installed* binaries, so build **and install**.

```bash
cd <build-dir> && ninja install
pip3 install -r <source>/ns_server/cluster_tests/requirements.txt
```

### Run through the wrapper

Use [`engines/ep/scripts/nightmare_cluster_tests.py`](../engines/ep/scripts/nightmare_cluster_tests.py)
in place of `run.py`. It takes the same arguments, plus `--timeout-scale` and
`--keep-auto-failover` (below):

```bash
cd <source>/ns_server/cluster_tests
export COUCHBASE_NIGHTMARE_MODE=<source>/kv_engine/engines/ep/nightmare_configuration.json
python3 <source>/kv_engine/engines/ep/scripts/nightmare_cluster_tests.py \
    -k --tests KVFileBasedRebalanceTests
```

With nightmare mode on, the wrapper:

* **Scales timeouts** by `--timeout-scale N` (default 10). cluster_tests'
  waits are sized for a normal build, and nightmare mode is about 5x slower, so
  without this tests fail on the clock. The affected waits are sample-bucket
  loads (120s), encryption-at-rest convergence (120s), rebalances (60s) and
  run.py's per-test watchdog (600s). A rebalance that runs out of time reports
  `Found: timeout`; that is the test giving up, not a failed rebalance.
* **Disables auto-failover** on each cluster it creates, because a node
  shutdown can take minutes (see [Known failures](#known-failures)).
  `--keep-auto-failover` opts out.
* **Excludes `NodeRemapTest`** (`EXCLUDED_TESTSETS`), both from discovery and
  from any `--tests` list. If nothing is left to run it exits with status 1.

With `COUCHBASE_NIGHTMARE_MODE` unset, the same command is a plain baseline
run. That is the quickest way to check whether a failure is caused by
nightmare mode at all.

The wrapper patches testlib functions by name, so if ns_server refactors them
it fails loudly at start-up. Timeout scaling built into `run.py` would be the
durable fix.

### Verify activation

Nightmare mode applies per bucket, at bucket start, so a testset that never
creates a bucket exercises none of it however many tests pass. Count the
activations, not the tests. Run with `-k`, because the data dir and its logs are
deleted on success:

```bash
grep -rho 'Nightmare mode enabled' test_cluster_data-*/ | wc -l
```

`0` means nothing was exercised. `KVRebalanceTests`, for example, never creates
a bucket; use `KVFileBasedRebalanceTests` instead.

### Group runs by node count

`run.py` reuses one cluster for every testset whose requirements it satisfies.
So a 3-node testset in a list drags 1-node testsets onto 3 nodes, and one failed
cluster start (about 15 minutes) takes all of them down. Run each node-count
group, and each rebalance testset, in its own invocation.

`run.py` rejects the whole `--tests` list if any name is unknown. It does so
before starting a cluster and suggests close matches, so a typo costs nothing;
`./run.py --list` shows every registered testset.

### Clean up before every run

Leftover processes from an earlier run are the most common cause of spurious
failures. The sign is a node reporting `UP` within milliseconds (something is
still listening) while a later one gets `Connection refused`. Killing memcached
alone is not enough, because the babysitter respawns it and other services
linger too. Kill everything run from the install directory:

```bash
pkill -9 -f "<source>/install/"
find . -maxdepth 1 -name 'test_cluster_data-*' -exec rm -rf {} +   # safe in zsh
ps aux | grep "<source>/install/" | grep -v grep | wc -l             # expect 0
```