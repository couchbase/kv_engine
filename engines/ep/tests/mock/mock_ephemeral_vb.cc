#include "config.h"

#include "mock_ephemeral_vb.h"

#include "ephemeral_tombstone_purger.h"
#include "failover-table.h"

size_t MockEphemeralVBucket::markOldTombstonesStale(rel_time_t purgeAge) {
    // Mark all deleted items in the HashTable which can be purged as Stale -
    // this removes them from the HashTable, transferring ownership to
    // SequenceList.

    HTTombstonePurger purger(purgeAge);
    purger.setCurrentVBucket(*this);
    ht.visit(purger);

    return purger.getNumItemsMarkedStale();
}
