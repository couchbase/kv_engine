/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include "collections/flush.h"
#include "../kvstore.h"
#include "collections/collection_persisted_stats.h"
#include "collections/vbucket_manifest.h"
#include "collections/vbucket_manifest_handles.h"
#include "ep_bucket.h"
#include "item.h"

namespace Collections::VB {

// Helper class for doing collection stat updates
class StatsUpdate {
public:
    explicit StatsUpdate(CachingReadHandle&& handle)
        : handle(std::move(handle)) {
    }

    /**
     * An item is being inserted into the collection
     * @param isCommitted the prepare/commit state of the item inserted
     * @param isDelete alive/delete stats of the item inserted
     * @param diskSizeDelta the +/- bytes the insert changes disk-used by
     */
    void insert(bool isCommitted, bool isDelete, ssize_t diskSizeDelta);

    /**
     * An item is being updated in the collection
     * @param isCommitted the prepare/commit state of the item updated
     * @param diskSizeDelta the +/- bytes the update changes disk-used by
     */
    void update(bool isCommitted, ssize_t diskSizeDelta);

    /**
     * An item is being removed (deleted) from the collection
     * @param isCommitted the prepare/commit state of the item removed
     * @param diskSizeDelta the +/- bytes the delete changes disk-used by
     */
    void remove(bool isCommitted, ssize_t diskSizeDelta);

    /**
     * @return true if the seqno represents a logically deleted item for the
     *         locked collection.
     */
    bool isLogicallyDeleted(uint64_t seqno) const;

    /**
     * Increment the 'disk' count for the collection associated with the key
     */
    void incrementDiskCount();

    /**
     * Decrement the 'disk' count for the collection associated with the key
     */
    void decrementDiskCount();

    /**
     * Update the on disk size (bytes) for the collection associated with
     * the key
     */
    void updateDiskSize(ssize_t delta);

private:
    /// handle on the collection
    CachingReadHandle handle;
};

bool StatsUpdate::isLogicallyDeleted(uint64_t seqno) const {
    return handle.isLogicallyDeleted(seqno);
}

void StatsUpdate::incrementDiskCount() {
    if (!handle.getKey().isInSystemCollection()) {
        handle.incrementDiskCount();
    }
}

void StatsUpdate::decrementDiskCount() {
    if (!handle.getKey().isInSystemCollection()) {
        handle.decrementDiskCount();
    }
}

void StatsUpdate::updateDiskSize(ssize_t delta) {
    handle.updateDiskSize(delta);
}

static std::optional<StatsUpdate> tryTolockAndSetPersistedSeqno(
        Flush& flush, const DocKey& key, uint64_t seqno, bool isCommitted) {
    if (key.isInSystemCollection()) {
        // Is it a collection system event?
        auto [event, id] = SystemEventFactory::getTypeAndID(key);
        switch (event) {
        case SystemEvent::Collection: {
            auto handle =
                    flush.getManifest().lock(key, Manifest::AllowSystemKeys{});
            if (handle.setPersistedHighSeqno(seqno)) {
                // Update the 'mutated' set, stats are changing
                flush.setMutated(CollectionID(id));
            } else {
                // Cannot set the seqno (flushing dropped items) no more updates
                return {};
            }
            return StatsUpdate{std::move(handle)};
        }
        case SystemEvent::Scope:
            break;
        }
        return {};
    }

    auto handle = flush.getManifest().lock(key);

    if (isCommitted) {
        if (handle.setPersistedHighSeqno(seqno)) {
            // Update the 'mutated' set, stats are changing
            flush.setMutated(key.getCollectionID());
            return StatsUpdate{std::move(handle)};
        } else {
            // Cannot set the seqno (flushing dropped items) no more updates
            return {};
        }
    }

    return StatsUpdate{std::move(handle)};
}

void StatsUpdate::insert(bool isCommitted,
                         bool isDelete,
                         ssize_t diskSizeDelta) {
    if (!isDelete && isCommitted) {
        incrementDiskCount();
    } // else inserting a tombstone or it's a prepare

    if (isCommitted) {
        updateDiskSize(diskSizeDelta);
    }
}

void StatsUpdate::update(bool isCommitted, ssize_t diskSizeDelta) {
    if (isCommitted) {
        updateDiskSize(diskSizeDelta);
    }
}

void StatsUpdate::remove(bool isCommitted, ssize_t diskSizeDelta) {
    if (isCommitted) {
        decrementDiskCount();
    } // else inserting a tombstone or it's a prepare

    if (isCommitted) {
        updateDiskSize(diskSizeDelta);
    }
}

void Flush::saveCollectionStats(
        std::function<void(CollectionID, PersistedStats)> cb) const {
    for (const auto c : mutated) {
        PersistedStats stats;
        {
            auto lock = manifest.lock(c);
            if (!lock.valid()) {
                // Can be flushing for a dropped collection (no longer in the
                // manifest)
                continue;
            }
            stats = lock.getPersistedStats();
        }
        cb(c, stats);
    }
}

void Flush::checkAndTriggerPurge(Vbid vbid, KVBucket& bucket) const {
    if (needsPurge) {
        triggerPurge(vbid, bucket);
    }
}

void Flush::triggerPurge(Vbid vbid, KVBucket& bucket) {
    CompactionConfig config;
    config.db_file_id = vbid;
    bucket.scheduleCompaction(vbid, config, nullptr);
}

void Flush::updateStats(const DocKey& key,
                        uint64_t seqno,
                        bool isCommitted,
                        bool isDelete,
                        size_t size) {
    auto update = tryTolockAndSetPersistedSeqno(*this, key, seqno, isCommitted);
    if (update) {
        update->insert(isCommitted, isDelete, size);
    }
}

void Flush::updateStats(const DocKey& key,
                        uint64_t seqno,
                        bool isCommitted,
                        bool isDelete,
                        size_t size,
                        uint64_t oldSeqno,
                        bool oldIsDelete,
                        size_t oldSize) {
    auto update = tryTolockAndSetPersistedSeqno(*this, key, seqno, isCommitted);
    if (update) {
        if (update->isLogicallyDeleted(oldSeqno) || oldIsDelete) {
            update->insert(isCommitted, isDelete, size);
        } else if (!oldIsDelete && isDelete) {
            update->remove(isCommitted, size - oldSize);
        } else {
            update->update(isCommitted, size - oldSize);
        }
    }
}

void Flush::setMutated(CollectionID cid) {
    mutated.insert(cid);
}

} // namespace Collections::VB