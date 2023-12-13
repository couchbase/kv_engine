/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "dcp_expiration.h"
#include "engine_wrapper.h"
#include "executors.h"
#include "utilities.h"
#include <memcached/limits.h>
#include <memcached/protocol_binary.h>
#include <xattr/blob.h>

static bool invalidXattrSize(cb::const_byte_buffer value,
                             protocol_binary_datatype_t datatype) {
    const char* payload = reinterpret_cast<const char*>(value.data());
    cb::xattr::Blob blob({const_cast<char*>(payload), value.size()},
                         cb::mcbp::datatype::is_snappy(datatype));
    return blob.get_system_size() > cb::limits::PrivilegedBytes;
}

void dcp_expiration_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    auto& connection = cookie.getConnection();
    if (ret == cb::engine_errc::success) {
        const auto& req = cookie.getRequest();
        const auto datatype = uint8_t(req.getDatatype());
        using cb::mcbp::request::DcpExpirationPayload;
        const auto& extras = req.getCommandSpecifics<DcpExpirationPayload>();

        auto value = req.getValue();

        if (cb::mcbp::datatype::is_xattr(datatype) &&
            invalidXattrSize(value, datatype)) {
            ret = cb::engine_errc::too_big;
        } else {
            ret = dcpExpiration(cookie,
                                req.getOpaque(),
                                connection.makeDocKey(req.getKey()),
                                value,
                                uint8_t(req.getDatatype()),
                                req.getCas(),
                                req.getVBucket(),
                                extras.getBySeqno(),
                                extras.getRevSeqno(),
                                extras.getDeleteTime());
        }
    }

    if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}
