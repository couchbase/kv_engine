/*
 *     Copyright 2025-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp_cached_value.h"
#include "daemon/cookie.h"
#include "engine_wrapper.h"
#include "executors.h"

#include <memcached/limits.h>
#include <memcached/protocol_binary.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

static cb::engine_errc dcp_cached_value(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    const auto& extras =
            req.getCommandSpecifics<cb::mcbp::request::DcpMutationPayload>();
    const auto datatype = uint8_t(req.getDatatype());
    const auto value = req.getValue();

    if (cb::mcbp::datatype::is_xattr(datatype)) {
        const char* payload = reinterpret_cast<const char*>(value.data());
        cb::xattr::Blob blob({const_cast<char*>(payload), value.size()},
                             cb::mcbp::datatype::is_snappy(datatype));
        if (blob.get_system_size() > cb::limits::PrivilegedBytes) {
            return cb::engine_errc::too_big;
        }
    }

    return dcpCachedValue(cookie,
                          req.getOpaque(),
                          cookie.getConnection().makeDocKey(req.getKey()),
                          value,
                          datatype,
                          req.getCas(),
                          req.getVBucket(),
                          extras.getFlags(),
                          extras.getBySeqno(),
                          extras.getRevSeqno(),
                          extras.getExpiration(),
                          extras.getNru());
}

static cb::engine_errc dcp_cached_key_meta(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    const auto& extras =
            req.getCommandSpecifics<cb::mcbp::request::DcpMutationPayload>();
    const auto datatype = uint8_t(req.getDatatype());

    return dcpCachedKeyMeta(cookie,
                            req.getOpaque(),
                            cookie.getConnection().makeDocKey(req.getKey()),
                            datatype,
                            req.getCas(),
                            req.getVBucket(),
                            extras.getFlags(),
                            extras.getBySeqno(),
                            extras.getRevSeqno(),
                            extras.getExpiration());
}

static cb::engine_errc dcp_cache_transfer_end(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    return dcpCacheTransferEnd(cookie, req.getOpaque(), req.getVBucket());
}

void dcp_cached_value_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        ret = dcp_cached_value(cookie);
    }

    if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}

void dcp_cached_key_meta_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        ret = dcp_cached_key_meta(cookie);
    }

    if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}

void dcp_cache_transfer_end_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        ret = dcp_cache_transfer_end(cookie);
    }

    // only respond on failure.
    if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}