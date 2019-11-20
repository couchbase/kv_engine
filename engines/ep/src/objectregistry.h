/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#pragma once

#include <atomic>
#include <cstddef>

class EventuallyPersistentEngine;
class Blob;
class Item;
class StoredValue;

extern "C" {
    typedef size_t (*get_allocation_size)(const void *ptr);
}

class StoredValue;

class ObjectRegistry {
public:
    static void initialize(get_allocation_size func);

    /**
     * Resets the ObjectRegistry back to initial state (before initialize()
     * was called).
     */
    static void reset();

    static void onCreateBlob(const Blob *blob);
    static void onDeleteBlob(const Blob *blob);

    static void onCreateItem(const Item *pItem);
    static void onDeleteItem(const Item *pItem);

    static void onCreateStoredValue(const StoredValue *sv);
    static void onDeleteStoredValue(const StoredValue *sv);


    static EventuallyPersistentEngine *getCurrentEngine();

    static EventuallyPersistentEngine *onSwitchThread(EventuallyPersistentEngine *engine,
                                                      bool want_old_thread_local = false);
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
    BucketAllocationGuard(EventuallyPersistentEngine* engine);
    ~BucketAllocationGuard();

private:
    EventuallyPersistentEngine* previous = nullptr;
};