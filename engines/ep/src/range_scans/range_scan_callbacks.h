/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#pragma once
#include "callbacks.h"

class EPBucket;
class RangeScan;
class VBucket;

/**
 * RangeScanDataHandlerIFace is an abstract class defining two methods that are
 * invoked as key/items of the scan are read from disk/cache. The methods can
 * be implemented depending on the execution context, unit-tests for example can
 * just store data in simple containers for inspection whilst the full stack can
 * use a handler which frames and sends data to the client.
 *
 * For scans that are configured as Key only handleKey is invoked
 * For scans that are configured as Key/Value handleItem is invoked
 *
 * A single scan will never call a mixture of the functions
 *
 */
class RangeScanDataHandlerIFace {
public:
    virtual ~RangeScanDataHandlerIFace() = default;

    /// @param key A key read from a Key only scan
    virtual void handleKey(DocKey key) = 0;

    /// @param item An Item read from a Key/Value scan
    virtual void handleItem(std::unique_ptr<Item> item) = 0;
};

/**
 * callback class which is invoked first by the scan and given each key (and
 * metadata). This class will check with the hash-table to see if the value is
 * available, allowing the scan to skip reading a value from disk
 */
class RangeScanCacheCallback : public StatusCallback<CacheLookup> {
public:
    RangeScanCacheCallback(const RangeScan& scan,
                           EPBucket& bucket,
                           RangeScanDataHandlerIFace& handler);

    void callback(CacheLookup& lookup) override;

protected:
    GetValue get(VBucket& vb, CacheLookup& lookup);
    const RangeScan& scan;
    EPBucket& bucket;
    RangeScanDataHandlerIFace& handler;
};

/**
 * callback class which is invoked by the scan if no value is available in the
 * cache. The full Item is passed to the callback.
 */
class RangeScanDiskCallback : public StatusCallback<GetValue> {
public:
    RangeScanDiskCallback(const RangeScan& scan,
                          RangeScanDataHandlerIFace& handler);

    void callback(GetValue& val) override;

protected:
    const RangeScan& scan;
    RangeScanDataHandlerIFace& handler;
};
