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

#include <atomic>
#include <cstddef>

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
    explicit BucketAllocationGuard(EventuallyPersistentEngine* engine);
    ~BucketAllocationGuard();

private:
    EventuallyPersistentEngine* previous = nullptr;
};