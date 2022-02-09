# High-Fidelity Most Frequently Used Eviction Policy

version 1.0

Daniel Owen

## Introduction

The High-Fidelity Most Recently Used Eviction (Hifi-MFU) policy replaces the
2-bit Least Recently Used eviction (2-bit_LRU) policy in 5.5.0 (Vulcan).

When memory usage reaches the high watermark, the `ItemPager` begins iterating
over all `StoredValue`s in memory, selecting values to _evict_. Evicting
a `StoredValue` either removes just the document body from memory (value
eviction)
or removes the body _and_ metadata (full eviction). Evicted documents are
_non-resident_ - they exist on disk but are not currently held in memory.

Eviction reduces memory usage, but at a cost: to serve a GET request for a
non-resident item, it must first be loaded from disk by a _background fetch_
(aka bgfetch). This increases the time taken to complete the GET operation.

To minimise how often items need to be bgfetched from disk, eviction attempts to
keep frequently accessed values in the `HashTable`. To efficiently track how
frequently value are used, Hifi-MFU maintains a 8-bit probabilistic frequency
counter for each `StoredValue`, which is updated when the value is read or
written.

The `ItemPager` will evict items if their frequency counter is below a certain
threshold. This threshold is determined by maintaining
a [histogram](#frequency-counter-histogram) of seen counter values. The
threshold is then set such that items with a frequency counter in the lowest N%
of items will be evicted - the least frequently accessed items.

As clients can continue to store data, after the pager has visited
all `StoredValue`s, memory usage might not have reached the low watermark. In
that case, the pager will start again, and will continue running until the low
watermark is reached.

### Tagged Pointer

Given an 8-bit frequency counter needs to be maintained for each StoredValue it
could potentially increase memory usage by a significant amount. Therefore we
make use of Tagged Pointers to avoid needing extra memory.

On x86-64 architectures the top 16-bits of a 64-bit address are not used.
Therefore it is possible for use to make use of this unused space to store data.
There are some complexities, such as the top 16-bits must be zero before a
64-bit pointer can be used to address a location in memory. Therefore a Tagged
Pointer wrapper class is provided to hide away this additional complexity.

There are two 64-bit pointers in the storedValue object `value`
and `chain_next_or_replacement`. Therefore we have a total of 32-bit of space
that we could exploit. The `value` pointer is of type
`SingleThreadedRCPtr<Blob>` whilst the `chain_next_or_replacement` pointer
is of type `std::unique_ptr<StoredValue>` therefore two variants of TaggedPtr
have been created. See *tagged_ptr_test.cc* for examples of how to use both.

We restrict our frequency counter to using only the bottom 8-bits of the 'value'
pointer. The top 8-bits are used to store an age (measured in defragmenter task
passes), which is used to determine if the defragger should be triggered.

## Frequency Count

We use a 8-bit counter held in the storedValue to record the frequency that an
item is accessed. The counter value is incremented using probabilistic counter
which means that calling the increment function will not always increase the
counter value. The behaviour is such that it is harder to increment the higher
the counter value is. This allows us to represent a large frequency counter
range in only a few bits.

The probabilistic counter function has to be invoked approximately 65K times
before the 8-bit counter will become saturated. This means the counter behaves
similar to a unsigned 16-bit counter, which has a maximum value of 65,536,
however only requires 8-bits of storage.

### Default Value

An item is initially created with a *initialFreqCount* which is set to a default
of 4. This ensures that the item is not an immediate candidate for eviction.

### Frequency Counter Histogram

When the item pager task is run, it triggers the paging visitor task which
iterates through all the vbucket selecting items for eviction. For each vbucket
we start with an initially empty frequency count histogram, and as we visit
items their frequency count gets added to the histogram.

The histogram attempts to approximate the distribution of frequency counter
values for eligible stored values; if an item is _ineligibie_ for eviction, for
example if it is *dirty* (not yet persisted to disk) or already non-resident, it
is not included in the histogram.

The paging visitor is provided a **percent** of items to evict from a given
vbucket. The **percent** is used to select a given frequency count threshold
from the frequency count histogram. When each item is visited in the
PagingVisitor it own frequency count is compared against the frequency count
threshold. If it less than or equal to the threshold then the item is considered
eligible for eviction.

The calculated percentage differs based on whether the vbucket is active or
replica.

### Active verses Replica Eviction

Eviction prioritises keeping active data resident.

The `ItemPager` attempts to reduce memory usage to the low watermark. The
approximate amount of memory which could be recovered from a vbucket if every
value were to be evicted can be determined ahead of time. If it is estimated
that the low watermark could be reached by evicting only replica data, no active
data will be evicted in the current `ItemPager`
pass.

This attempts to maximise the amount of resident active data, minimising how
often client operations need to read data from disk.

### Decaying Task

Couchbase is expected to run for months, even years, without being restarted
therefore it is possible that after an extended perod of time the frequency
counts would become saturated for a large number of items. To avoid this a
decayer task runs when the frequency counter of an item becomes saturated.

Frequency counters are decayed in 2 ways:

* If we iterate over an item during the ItemPager run and we do not evict, we
  reduce its frequency count by 1. This ensures that if there is no changes to
  the hash table the item will evently be evicted.
* If the frequency count of an item in the bucket get saturated we run the
  decayerTask. This task iterates over all items and reduces the frequency
  counts by 50%.

## Aging

The aging component is used to ensure that items that are recently added to the
hash table are not immediately evicted.

Age is measured by taking the item's current cas from the maxCas (which is the
maximum cas value of the associated vbucket). The time in nanoseconds is stored
in the top 48 bits of the cas therefore we shift the age by 16-bits. This allows
us to have an age histogram with a reduced maximum value and therefore reduces
the memory requirements.

### Age Histogram

The age histogram is populated by adding the current age of each item that we
visit during PagingVisitor::visit.

The PagingVisitor constructor is passed an agePercentage (default of 30%) and a
freqCounterAgeThreshold, which is a value between 0 and 255 (defaulted to 1).

The agePercentage is used to select a given age from the age histogram. The
threshold returned from the histogram is used to define the minimum age that an
item can be condidered for eviction. For example if histogram when selected at
30% returns the value 10, then only those items that have an age >= to 10 will
be eligible of eviction. Those below the threshold are considered too young for
eviction.

To ensure that items do not remain in the hash table for longer than required,
there is a minimum frequency counter threshold at which age is ignored (
defaulted to 1). This means that items that have a frequency counter below 1 (
i.e. 0)
do not take age into account when being considered for eviction.

## Updating The Frequency Counter Threshold and Age Threshold

As the frequency histogram and age histogram take time to be constructed, there
is a learning phase. During this learning phase both the frequency counter and
age thresholds are updated each time an item is visited. The duration of the
learning phase is determined to be when the frequency counter histogram
contains <= to the `learningPopulation`. The `learningPopulation` is determined
by a constant value, currently set to 100.

Once outside of the learning phase the two thresholds are updating periodically.
The frequency of updating the threshold is set
to `vbucket->getNumItems() * 0.001`. So if the vbucket contains 10K items then
every time the frequency histogram contains 10 more items the thresholds will be
updated.

## Detailed Algorithm

When the `ItemPager` task runs, the current memory usage is compared the _high_
water mark. If it is greater, eviction will continue until memory usage falls
below the _low_ water mark. This may lead to a "sawtooth" pattern in memory
usage over time.

The percentage of data to evict from active and replica vbuckets is computed
as (roughly)

```

memoryToRecover = currentMemoryUsage - lowWaterMark
replicaEvictionRatio = min(1.0, memoryToRecover / replicaEvictableMemory)
activeEvictionRatio = max(0, (memoryToRecover-replicaEvictableMemory) / activeEvictableMemory)
```

The `ItemPager` creates one or more `PagingVisitor`s (
see [concurrent_pagers](#configuration-parameters)), which will visit vbuckets
in descending order of memory usage.

For each vbucket, the `PagingVistor` then visits each `StoredValue` in the
vbucket's `HashTable`. If the value is dirty[^1] or non-resident it is skipped.

[^1]: Dirty items have not yet been persisted; until they are, the value must
remain in the HashTable. Otherwise an incoming `GET` would see either no value,
or would bgfetch an _old_ version of the document from disk - this would break
consistency.

Otherwise, the value's frequency counter is compared
to `frequencyCountThreshold`. If it is _above_ the threshold, the value is
skipped - it is "frequently used".

The frequency counter is then compared to `freqCounterAgeThreshold` (which is a
configuration parameter, defaulting to 1). If _below_, the age of the value is
ignored - it is "cold" enough to evict regardless of age. Otherwise, the age of
the item (taken from the CAS) is compared to `ageThreshold`, if below the item
is "young" and is skipped.

If the item passes all the checks an attempt is made to evict it, and its
frequency count and age are stored in the appropriate histograms.

If we are in the learning phase (i.e. first 100) or periodically we update the
frequency count threshold and age threshold.

Once all values for a given vbucket have been visited, memory usage is
re-checked. If it is now below the low watermark, the visitor stops.

## Propagation of Hotness Information

If the consumer supports the hifi mfu eviction algorithm then the producer will
send the frequency counter value of the item. Otherwise it will be converted
from the 255 possible values to one of the 4 states used in the pre-existing Not
Recently Used (NRU) algorithm.

The hotness information is sent from the producer to the consumer on each
PREPARE and MUTATION. Note: that because it is only sent on write operations,
items on the replica may not have an up-to-date frequency count.

## Configuration Parameters

* **ht\_eviction\_policy** - provides the ability to swtich between the original
  2-bit\_lru policy and the hifi_mfu policy.
* **item\_eviction\_age\_percentage** - defines the percentage that is used to
  select the appropriate age value from the age histogram, (defaulted to 30%).
* **item\_eviction\_freq\_counter\_age\_threshold** - defines the frequency
  counter value at which the age is ignored, (
  defaulted to 1).
* **item\_freq\_decayer\_chunk\_duration** - defines time (in ms)
  itemFreqDecayer task will run for before being paused, defaulted to 20ms.
* **item\_freq\_decayer\_percent** - defines the percent that the frequency
  counter of a document is decayed when visited by item\_freq\_decayer, (
  defaulted to 50%).
* **concurrent\_pagers** - number of paging visitors created by the `ItemPager`.
  Controls how many vbuckets can be visited at the same time. (defaulted to 2).

## Stats

cbstats can be used with the **eviction** parameter to get the histogram of the
number of docs evicted at specific frequency counts, as well as a snapshot of
the current frequency count histogram. For example, running the follow cbstats
command on `default` bucket results in the following output.

`./cbstats 127.0.0.1:11210 -u <USERNAME> -p <PASSWORD> -b default eviction`

`ep_active_or_pending_eviction_values_evicted (306849 total)`

    0 - 0 : ( 43.9780%) 134946 █████████████████████████████████████████████████▋
    1 - 1 : ( 46.9573%)   9142 ███▎
    2 - 2 : ( 50.6001%)  11178 ████
    3 - 3 : ( 57.9484%)  22548 ████████▎
    4 - 4 : (100.0000%) 129035 ███████████████████████████████████████████████▌
    Avg   : (    0.0)

The first bar chart shows the total number of evictions from active or pending
vbuckets and a histogram of frequency counts when documents were evicted. If a
replica was configured for the bucket, a similar histogram would be presented
for replica vbuckets.

The second histogram produced is a snapshot of the current frequency count
histogram for the last active or pending vbucket from which documents were
evicted. Again if a replica was configured for the bucket, a similar histogram
would be shown for the last replica vbucket from which documents were evicted.

`ep_active_or_pending_eviction_values_snapshot (5241 total)`

    0 - 0     : (  1.4119%)   74 █▌
    1 - 1     : (  3.0338%)   85 █▊
    2 - 2     : (  4.5602%)   80 █▋
    3 - 3     : (  6.0103%)   76 █▌
    4 - 4     : (  7.5558%)   81 █▋
    255 - 255 : (100.0000%) 4845 ██████████████████████████████████████████████████████████████████████████████████████████████████████▌
    Avg       : (    0.0)

