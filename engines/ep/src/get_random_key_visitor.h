/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once

#include "item.h"
#include "kv_bucket.h"
#include "vb_visitors.h"

class StoredValue;

/**
 * An observer interface for GetRandomKeyVisitor, which allows the caller to be
 * notified of the progress of the visitor, and to be informed when a random
 * key has been found.
 */
struct GetRandomKeyObserver {
    virtual ~GetRandomKeyObserver();
    /// Called when the visitor starts visiting vbuckets
    virtual void start() = 0;
    /// Called when the visitor completed visiting vbuckets
    virtual void finish() = 0;
    /// Called when the visitor found what it was looking for, providing the
    /// found item. After this call the visitor will stop visiting vbuckets.
    virtual void found(std::unique_ptr<Item> item) = 0;
    /// Called when the visitor encountered an error, providing the error code
    /// and the manifest UUID of the vbucket which caused the error. After this
    /// call the visitor will stop visiting vbuckets.
    virtual void error(cb::engine_errc error, uint64_t manifest_uuid) = 0;
};

/**
 * A visitor which visits the hash tables of all vbuckets in a random order,
 * looking for a random key.
 */
class GetRandomKeyVisitor : public CappedDurationVBucketVisitor {
public:
    /**
     * Creates a GetRandomKeyVisitor.
     *
     * @param engine The engine which will be used to notify the caller when the
     *               visitor has completed.
     * @param cookie The cookie which will be used to notify the caller when the
     *               visitor has completed.
     * @param cid The collection ID to look for the random key in.
     * @param observer The observer which will be notified of the progress of
     *                 the visitor, and when a random key has been found.
     */
    GetRandomKeyVisitor(EventuallyPersistentEngine& engine,
                        CookieIface& cookie,
                        CollectionID cid,
                        GetRandomKeyObserver& observer);
    ~GetRandomKeyVisitor() override;
    void visitBucket(VBucket& vb) override;
    void complete() override;
    ExecutionState shouldInterrupt() override;
    VisitPolicy getVisitPolicy() override {
        return VisitPolicy::Random;
    }

protected:
    EventuallyPersistentEngine& engine;
    CookieIface& cookie;
    const CollectionID cid;
    GetRandomKeyObserver& observer;
    bool stop = false;
};
