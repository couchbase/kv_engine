/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "objectregistry.h"

#include "ep_engine.h"
#include "item.h"
#include "stored-value.h"
#include <platform/cb_arena_malloc.h>

static thread_local EventuallyPersistentEngine* th = nullptr;

static bool verifyEngine(EventuallyPersistentEngine* engine) {
    if (engine == nullptr) {
        static const char* allowNoStatsUpdate = getenv("ALLOW_NO_STATS_UPDATE");
        if (allowNoStatsUpdate) {
            return false;
        } else {
            throw std::logic_error("verifyEngine: engine should be non-NULL");
        }
    }
    return true;
}

void ObjectRegistry::onCreateBlob(const Blob* blob) {
    EventuallyPersistentEngine* engine = th;
    if (verifyEngine(engine)) {
        auto& coreLocalStats = engine->getEpStats().coreLocal.get();

        size_t size = cb::ArenaMalloc::malloc_usable_size(blob);
        coreLocalStats->blobOverhead.fetch_add(size - blob->getSize());
        coreLocalStats->currentSize.fetch_add(size);
        coreLocalStats->totalValueSize.fetch_add(size);
        coreLocalStats->numBlob++;
    }
}

void ObjectRegistry::onDeleteBlob(const Blob* blob) {
    EventuallyPersistentEngine* engine = th;
    if (verifyEngine(engine)) {
        auto& coreLocalStats = engine->getEpStats().coreLocal.get();

        size_t size = cb::ArenaMalloc::malloc_usable_size(blob);
        coreLocalStats->blobOverhead.fetch_sub(size - blob->getSize());
        coreLocalStats->currentSize.fetch_sub(size);
        coreLocalStats->totalValueSize.fetch_sub(size);
        coreLocalStats->numBlob--;
    }
}

void ObjectRegistry::onCreateStoredValue(const StoredValue* sv) {
    EventuallyPersistentEngine* engine = th;
    if (verifyEngine(engine)) {
        auto& coreLocalStats = engine->getEpStats().coreLocal.get();

        size_t size = cb::ArenaMalloc::malloc_usable_size(sv);
        coreLocalStats->numStoredVal++;
        coreLocalStats->totalStoredValSize.fetch_add(size);
    }
}

void ObjectRegistry::onDeleteStoredValue(const StoredValue* sv) {
    EventuallyPersistentEngine* engine = th;
    if (verifyEngine(engine)) {
        auto& coreLocalStats = engine->getEpStats().coreLocal.get();

        size_t size = cb::ArenaMalloc::malloc_usable_size(sv);
        coreLocalStats->totalStoredValSize.fetch_sub(size);
        coreLocalStats->numStoredVal--;
    }
}

void ObjectRegistry::onCreateItem(const Item* pItem) {
    EventuallyPersistentEngine* engine = th;
    if (verifyEngine(engine)) {
        auto& coreLocalStats = engine->getEpStats().coreLocal.get();
        coreLocalStats->memOverhead.fetch_add(pItem->size() -
                                              pItem->getValMemSize());
        ++coreLocalStats->numItem;
    }
}

void ObjectRegistry::onDeleteItem(const Item* pItem) {
    EventuallyPersistentEngine* engine = th;
    if (verifyEngine(engine)) {
        auto& coreLocalStats = engine->getEpStats().coreLocal.get();
        coreLocalStats->memOverhead.fetch_sub(pItem->size() -
                                              pItem->getValMemSize());
        --coreLocalStats->numItem;
    }
}

EventuallyPersistentEngine* ObjectRegistry::getCurrentEngine() {
    return th;
}

EventuallyPersistentEngine* ObjectRegistry::onSwitchThread(
        EventuallyPersistentEngine* engine, bool want_old_thread_local) {
    EventuallyPersistentEngine* old_engine = nullptr;

    if (want_old_thread_local) {
        old_engine = th;
    }

    // Set the engine so that onDeleteItem etc... can update their stats
    th = engine;

    // Next tell ArenaMalloc what todo so that we can account memory to the
    // bucket
    if (engine) {
        cb::ArenaMalloc::switchToClient(engine->getArenaMallocClient());
    } else {
        cb::ArenaMalloc::switchFromClient();
    }
    return old_engine;
}

NonBucketAllocationGuard::NonBucketAllocationGuard()
    : engine(ObjectRegistry::onSwitchThread(nullptr, true)) {
}

NonBucketAllocationGuard::~NonBucketAllocationGuard() {
    ObjectRegistry::onSwitchThread(engine);
}

BucketAllocationGuard::BucketAllocationGuard(EventuallyPersistentEngine* engine)
    : previous(ObjectRegistry::onSwitchThread(engine, true)) {
}

BucketAllocationGuard::~BucketAllocationGuard() {
    ObjectRegistry::onSwitchThread(previous);
}
