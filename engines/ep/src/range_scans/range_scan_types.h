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

#include <memcached/engine_error.h>
#include <memcached/range_scan_id.h>

#include <vector>

class CookieIface;

// RangeScanCreateState describes which stage of creation a RangeScan is in
// Creation always starts in Pending and then:
// 1) Pending->Creating for a scan with no seqno persistence requirements
// 2) Pending->WaitForPersistence->Creating for a scan that has seqno
//    persistence requirements
enum class RangeScanCreateState : char {
    Pending, // RangeScan creation begins
    WaitForPersistence, // RangeScan create is waiting for a seqno to be stored
    Creating // RangeScan has scheduled a task to create the scan
};

// Data stored in engine-specific during a RangeScan create request
struct RangeScanCreateToken {
    cb::rangescan::Id uuid;
    RangeScanCreateState state{RangeScanCreateState::Pending};
};

// Data stored in engine-specific during a RangeScan continue request
struct RangeScanContinueToken {
    cb::rangescan::Id uuid;
};

/**
 * Base class for the frontend executor RangeScan result. This class owns
 * the std::vector which stores mcbp formatted data from the scan. This class
 * also knows if the scan is key or value.
 *
 */
class RangeScanContinueResult {
public:
    virtual ~RangeScanContinueResult() = default;
    virtual void complete(CookieIface& cookie) = 0;

protected:
    RangeScanContinueResult(std::vector<uint8_t> buffer, bool keyOnly);

    void send(CookieIface& cookie, cb::engine_errc status);

    const std::vector<uint8_t> responseBuffer;
    const bool keyOnly{false};
};

/**
 * RangeScanContinueResultPartial is an object created when a RangeScan has
 * yielded because the send buffer is at or exceeding the configured size. In
 * this case the RangeScan will run again without any input from the client.
 */
class RangeScanContinueResultPartial : public RangeScanContinueResult {
public:
    RangeScanContinueResultPartial(std::vector<uint8_t> buffer, bool keyOnly);

    /**
     * Sends the buffered data using Cookie::sendResponse with a status code of
     * success.
     */
    void complete(CookieIface& cookie) override;
};

// Intermediate class which extends the baseclass with the read bytes
class RangeScanContinueResultWithReadBytes : public RangeScanContinueResult {
protected:
    RangeScanContinueResultWithReadBytes(std::vector<uint8_t> buffer,
                                         size_t readBytes,
                                         bool keyOnly);

    /**
     * Passes the value of readBytes to Cookie::addDocumentReadBytes
     */
    void complete(CookieIface& cookie) override;

    /// the number of bytes read so for the scan
    const size_t readBytes{0};
};

/**
 * Result class for when the range-scan is incomplete and has stopped for a
 * user defined limit. The scan will return the buffer and a status code of
 * range_scan_more
 */
class RangeScanContinueResultMore
    : public RangeScanContinueResultWithReadBytes {
public:
    RangeScanContinueResultMore(std::vector<uint8_t> buffer,
                                size_t readBytes,
                                bool keyOnly);

    /**
     * Sends the buffered data using Cookie::sendResponse with a status code of
     * range_scan_more.
     */
    void complete(CookieIface& cookie) override;
};

/**
 * Result class for when the range-scan is complete. The scan will return the
 * buffer and a status code of range_scan_complete
 */
class RangeScanContinueResultComplete
    : public RangeScanContinueResultWithReadBytes {
public:
    RangeScanContinueResultComplete(std::vector<uint8_t> buffer,
                                    size_t readBytes,
                                    bool keyOnly);
    /**
     * Sends the buffered data using Cookie::sendResponse with a status code of
     * range_scan_complete.
     */
    void complete(CookieIface& cookie) override;
};

/**
 * Result class for when the range-scan is cancelled. The buffer of data is
 * can be moved out of the RangeScan object and into this result for destruction
 * out-side of any locks. The readBytes value will be propagated to the cookie
 * when complete is invoked.
 */
class RangeScanContinueResultCancelled
    : public RangeScanContinueResultWithReadBytes {
public:
    RangeScanContinueResultCancelled(std::vector<uint8_t> buffer,
                                     size_t readBytes,
                                     bool keyOnly);

    /**
     * Does not send the buffered data, but only ensures the current readBytes
     * value is propagated to the cookie as it may get "attached" to the final
     * mcbp frame
     */
    void complete(CookieIface& cookie) override {
        RangeScanContinueResultWithReadBytes::complete(cookie);
    }
};
