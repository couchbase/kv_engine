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
#pragma once

#include <platform/cb_arena_malloc_client.h>

class EventuallyPersistentEngine;
class Blob;
class Item;
class StoredValue;

class ObjectRegistry {
public:
    static void onCreateBlob(const Blob* blob);
    static void onDeleteBlob(const Blob* blob);

    static void onCreateItem(const Item* pItem);
    static void onDeleteItem(const Item* pItem);

    static void onCreateStoredValue(const StoredValue* sv);
    static void onDeleteStoredValue(const StoredValue* sv);

    static EventuallyPersistentEngine* getCurrentEngine();

    static EventuallyPersistentEngine* onSwitchThread(
            EventuallyPersistentEngine* engine,
            bool want_old_thread_local = false,
            cb::MemoryDomain domain = cb::MemoryDomain::Primary);

    /**
     * Switch memory tracking for the calling thread to the specified engine
     * with the specified memory domain. Use of a nullptr will disable memory
     * tracking for the calling thread.
     *
     * @param engine the engine or nullptr to disable tracking
     * @param want_old_thread_local true if the old engine pointer is required
     *        this is an extra thread-local read which can be avoided.
     * @param domain the domain to set
     *
     * @return the previous engine (if want_old_thread_local=true) and domain
     */
    static std::pair<EventuallyPersistentEngine*, cb::MemoryDomain>
    switchToEngine(EventuallyPersistentEngine* engine,
                   bool want_old_thread_local = false,
                   cb::MemoryDomain domain = cb::MemoryDomain::Primary);
};

/**
 * To avoid mem accounting within a block
 */
class NonBucketAllocationGuard {
public:
    NonBucketAllocationGuard();
    ~NonBucketAllocationGuard();

private:
    EventuallyPersistentEngine* engine = nullptr;
};

/**
 * BucketAllocationGuard will place the given engine pointer into
 * ObjectRegistry and switch back to the previous state on destruction
 */
class BucketAllocationGuard {
public:
    explicit BucketAllocationGuard(EventuallyPersistentEngine* engine);
    ~BucketAllocationGuard();

private:
    EventuallyPersistentEngine* previous = nullptr;
};
