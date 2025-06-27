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
#include "engine_wrapper.h"
#include "executors.h"
#include "utilities.h"

#include <memcached/limits.h>
#include <memcached/protocol_binary.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

cb::engine_errc dcp_cached_value(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    const auto& extras =
            req.getCommandSpecifics<cb::mcbp::request::DcpMutationPayload>();
    const auto datatype = uint8_t(req.getDatatype());
    const auto nmeta = extras.getNmeta();
    auto value = req.getValue();
    const cb::const_byte_buffer meta = {value.data() + value.size() - nmeta,
                                        nmeta};
    value = {value.data(), value.size() - nmeta};

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
                          extras.getLockTime(),
                          meta,
                          extras.getNru());
}