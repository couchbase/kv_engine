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
#include "ep_types.h"

#include <folly/Synchronized.h>
#include <memcached/engine_error.h>

namespace Collections::VB {
class CachingReadHandle;
}

class CookieIface;
class EventuallyPersistentEngine;
class EPBucket;
class RangeScan;
class RangeScanContinueResult;
class StatCollector;
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
    enum class Status {
        OK, // Scan can continue
        Throttle, // Scan must yield because the connection is now throttled
        ExceededBufferLimit // Scan must yield because the send buffer is full
    };
    /**
     * Callback method invoked for each key that is read from the snapshot. This
     * is only invoked for a KeyOnly::Yes scan.
     *
     * @param key A key read from a Key only scan
     * @return A "Status" which determines the next "step" for the scan
     */
    virtual Status handleKey(DocKey key) = 0;

    /**
     * Callback method invoked for each Item that is read from the snapshot.
     * This is only invoked for a KeyOnly::No scan.
     *
     * @param item An Item read from a Key/Value scan
     * @return A "Status" which determines the next "step" for the scan
     */
    virtual Status handleItem(std::unique_ptr<Item> item) = 0;

    /**
     * Frontend executor thread will invoke this method after an IO complete
     * wakeup. This is for when the I/O task yielded due to the internal buffer
     * being full. The continue request is not finalised.
     * @return RangeScanContinueResult which will own the buffer of data that
     *         must be shipped to the client.
     */
    virtual std::unique_ptr<RangeScanContinueResult>
    continuePartialOnFrontendThread() = 0;

    /**
     * Frontend executor thread will invoke this method after an IO complete
     * wakeup. This is for when the I/O task yielded due to a limit being
     * reached. The continue request is now finalised.
     * @return RangeScanContinueResult which will own the buffer of data that
     *         must be shipped to the client.
     */
    virtual std::unique_ptr<RangeScanContinueResult>
    continueMoreOnFrontendThread() = 0;

    /**
     * Frontend executor thread will invoke this method after an IO complete
     * wakeup. This is for when the I/O task yielded because the scan has hit
     * the end of the range. The continue request is now finalised.
     * @return RangeScanContinueResult which will own the buffer of data that
     *         must be shipped to the client.
     */
    virtual std::unique_ptr<RangeScanContinueResult>
    completeOnFrontendThread() = 0;

    /**
     * Frontend executor thread will invoke this method after an IO complete
     * wakeup. This if for when the I/O task yielded because an error was
     * encountered and the scan is now cancelled. The continue request is now
     * finalised.
     *
     * @return RangeScanContinueResult which will own the buffer of data. The
     *         caller can then move the result to a place where the buffer can
     *         be freed (e.g. outside of any locks).
     */
    virtual std::unique_ptr<RangeScanContinueResult>
    cancelOnFrontendThread() = 0;

    /**
     * Generate stats from the handler
     */
    virtual void addStats(std::string_view prefix,
                          const StatCollector& collector) = 0;
};

/**
 * RangeScanDataHandler is the handler used to join the I/O task to a real
 * client/cookie (i.e. not unit-test code)
 */
class RangeScanDataHandler : public RangeScanDataHandlerIFace {
public:
    RangeScanDataHandler(EventuallyPersistentEngine& engine,
                         bool keyOnly,
                         bool includeXattrs);

    Status handleKey(DocKey key) override;

    Status handleItem(std::unique_ptr<Item> item) override;

    void addStats(std::string_view prefix,
                  const StatCollector& collector) override;

    std::unique_ptr<RangeScanContinueResult> continuePartialOnFrontendThread()
            override;

    std::unique_ptr<RangeScanContinueResult> continueMoreOnFrontendThread()
            override;

    std::unique_ptr<RangeScanContinueResult> completeOnFrontendThread()
            override;

    std::unique_ptr<RangeScanContinueResult> cancelOnFrontendThread() override;

private:
    /**
     * @return the status of the scan based on the amount of buffered data
     */
    Status getScanStatus(size_t bufferedSize);

    /**
     * Data read from the scan is stored in the following vector ready for
     * sending. When sendTriggerThreshold is reached a continue will yield and
     * the frontend connection thread can transmit the contents of the
     * responseBuffer.
     *
     * The pendingReadBytes member is needed to support throttling and both
     * frontend and IO threads will need to access this variable.
     *
     * This is Synchronized as the frontend and IO tasks access it, however
     * there is no expectation that there will be contention for access.
     */
    struct RangeScanContinueBuffer {
        std::vector<uint8_t> responseBuffer;
        size_t pendingReadBytes{0};
    };
    folly::Synchronized<RangeScanContinueBuffer, std::mutex> scannedData;

    /// the trigger for pushing data to send, set from engine configuration
    const size_t sendTriggerThreshold{0};

    const bool keyOnly{false};

    const bool includeXattrs{false};
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

protected:
    /**
     * setScanErrorStatus is used for any !success status that the callback
     * concludes. The status will bring the scan to a halt and channel the
     * status code to the client via RangeScan::handleStatus
     */
    void setScanErrorStatus(cb::engine_errc status);

    /**
     * setUnknownCollection is a special case error path for unknown collection
     * which needs to pass the manifestUid of the failed collection lookup to
     * the response message.
     */
    void setUnknownCollection(uint64_t manifestUid);

    GetValue get(VBucketStateLockRef vbStateLock,
                 VBucket& vb,
                 Collections::VB::CachingReadHandle& cHandle,
                 CacheLookup& lookup);
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
