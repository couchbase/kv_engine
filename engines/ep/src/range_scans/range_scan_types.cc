/*
 *     Copyright 2023-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "range_scans/range_scan_types.h"

#include "objectregistry.h"

#include <mcbp/codec/range_scan_continue_codec.h>
#include <memcached/cookie_iface.h>
#include <memcached/engine_error.h>

RangeScanContinueResult::RangeScanContinueResult(std::vector<uint8_t> buffer,
                                                 bool keyOnly,
                                                 bool includeXattrs)
    : responseBuffer(std::move(buffer)),
      keyOnly(keyOnly),
      includeXattrs(includeXattrs) {
}

void RangeScanContinueResult::send(CookieIface& cookie,
                                   cb::engine_errc status) {
    cb::mcbp::response::RangeScanContinueResponseExtras extras(keyOnly);

    {
        NonBucketAllocationGuard guard;
        cookie.sendResponse(
                status,
                extras.getBuffer(),
                {reinterpret_cast<const char*>(responseBuffer.data()),
                 responseBuffer.size()});
    }
}

RangeScanContinueResultPartial::RangeScanContinueResultPartial(
        std::vector<uint8_t> buffer, bool keyOnly, bool includeXattrs)
    : RangeScanContinueResult(std::move(buffer), keyOnly, includeXattrs) {
}

void RangeScanContinueResultPartial::complete(CookieIface& cookie) {
    // This is a partially completed continue, the mcbp frame has success as
    // the status.
    RangeScanContinueResult::send(cookie, cb::engine_errc::success);
}

RangeScanContinueResultWithReadBytes::RangeScanContinueResultWithReadBytes(
        std::vector<uint8_t> buffer,
        size_t readBytes,
        bool keyOnly,
        bool includeXattrs)
    : RangeScanContinueResult(std::move(buffer), keyOnly, includeXattrs),
      readBytes(readBytes) {
}

void RangeScanContinueResultWithReadBytes::complete(CookieIface& cookie) {
    cookie.addDocumentReadBytes(readBytes);
}

RangeScanContinueResultMore::RangeScanContinueResultMore(
        std::vector<uint8_t> buffer,
        size_t readBytes,
        bool keyOnly,
        bool includeXattrs)
    : RangeScanContinueResultWithReadBytes(
              std::move(buffer), readBytes, keyOnly, includeXattrs) {
}

void RangeScanContinueResultMore::complete(CookieIface& cookie) {
    // This is a continue which reached a limit, but is not complete.
    RangeScanContinueResultWithReadBytes::complete(cookie);
    RangeScanContinueResult::send(cookie, cb::engine_errc::range_scan_more);
}

RangeScanContinueResultComplete::RangeScanContinueResultComplete(
        std::vector<uint8_t> buffer,
        size_t readBytes,
        bool keyOnly,
        bool includeXattrs)
    : RangeScanContinueResultWithReadBytes(
              std::move(buffer), readBytes, keyOnly, includeXattrs) {
}

void RangeScanContinueResultComplete::complete(CookieIface& cookie) {
    RangeScanContinueResultWithReadBytes::complete(cookie);
    RangeScanContinueResult::send(cookie, cb::engine_errc::range_scan_complete);
}

RangeScanContinueResultCancelled::RangeScanContinueResultCancelled(
        std::vector<uint8_t> buffer,
        size_t readBytes,
        bool keyOnly,
        bool includeXattrs)
    : RangeScanContinueResultWithReadBytes(
              std::move(buffer), readBytes, keyOnly, includeXattrs) {
}
