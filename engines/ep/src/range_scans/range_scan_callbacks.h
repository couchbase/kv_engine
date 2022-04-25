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

    /**
     * Callback method invoked for each key that is read from the snapshot. This
     * is only invoked for a KeyOnly::Yes scan.
     *
     *  @param key A key read from a Key only scan
     */
    virtual void handleKey(DocKey key) = 0;

    /**
     * Callback method invoked for each Item that is read from the snapshot.
     * This is only invoked for a KeyOnly::No scan.
     *
     *  @param item An Item read from a Key/Value scan
     */
    virtual void handleItem(std::unique_ptr<Item> item) = 0;

    /**
     * Callback method for when a scan has finished a "continue" and is used to
     * set the status of the scan. A "continue" can finish prematurely due to
     * an error or successfully because it has reached the end of the scan or
     * a limit.
     *
     * @param status The status of the just completed continue
     */
    virtual void handleStatus(cb::engine_errc status) = 0;
};

/**
 * RangeScanDataHandler is the handler used to join the I/O task to a real
 * client/cookie (i.e. not unit-test code)
 */
class RangeScanDataHandler : public RangeScanDataHandlerIFace {
public:
    void handleKey(DocKey key) override;

    void handleItem(std::unique_ptr<Item> item) override;

    void handleStatus(cb::engine_errc status) override;
};

/**
 * callback class which is invoked first by the scan and given each key (and
 * metadata). This class will check with the hash-table to see if the value is
 * available, allowing the scan to skip reading a value from disk
 */
class RangeScanCacheCallback : public StatusCallback<CacheLookup> {
public:
    RangeScanCacheCallback(RangeScan& scan, EPBucket& bucket);

    void callback(CacheLookup& lookup) override;

    /**
     * setScanErrorStatus is used for any !success status that the callback
     * concludes. The status will bring the scan to a halt and channel the
     * status code to the client via RangeScan::handleStatus
     */
    void setScanErrorStatus(cb::engine_errc status);

protected:
    GetValue get(VBucket& vb, CacheLookup& lookup);
    RangeScan& scan;
    EPBucket& bucket;
};

/**
 * callback class which is invoked by the scan if no value is available in the
 * cache. The full Item is passed to the callback.
 */
class RangeScanDiskCallback : public StatusCallback<GetValue> {
public:
    RangeScanDiskCallback(RangeScan& scan);

    void callback(GetValue& val) override;

    /**
     * setScanErrorStatus is used for any !success status that the callback
     * concludes. The status will bring the scan to a halt and channel the
     * status code to the client via RangeScan::handleStatus
     */
    void setScanErrorStatus(cb::engine_errc status);

protected:
    RangeScan& scan;
};
