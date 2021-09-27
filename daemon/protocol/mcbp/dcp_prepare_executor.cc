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

#include "engine_wrapper.h"
#include "executors.h"
#include "utilities.h"
#include <memcached/durability_spec.h>
#include <memcached/limits.h>
#include <memcached/protocol_binary.h>
#include <xattr/blob.h>

void dcp_prepare_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        const auto& req = cookie.getRequest();
        const auto& extras =
                req.getCommandSpecifics<cb::mcbp::request::DcpPreparePayload>();
        const auto datatype = uint8_t(req.getDatatype());
        const auto value = req.getValue();

        uint32_t priv_bytes = 0;
        if (mcbp::datatype::is_xattr(datatype)) {
            const char* payload = reinterpret_cast<const char*>(value.data());
            cb::xattr::Blob blob({const_cast<char*>(payload), value.size()},
                                 mcbp::datatype::is_snappy(datatype));
            priv_bytes = uint32_t(blob.get_system_size());
            if (priv_bytes > cb::limits::PrivilegedBytes) {
                ret = cb::engine_errc::too_big;
            }
        }

        if (ret == cb::engine_errc::success) {
            ret = dcpPrepare(cookie,
                             req.getOpaque(),
                             cookie.getConnection().makeDocKey(req.getKey()),
                             value,
                             priv_bytes,
                             datatype,
                             req.getCas(),
                             req.getVBucket(),
                             extras.getFlags(),
                             extras.getBySeqno(),
                             extras.getRevSeqno(),
                             extras.getExpiration(),
                             extras.getLockTime(),
                             extras.getNru(),
                             extras.getDeleted() ? DocumentState::Deleted
                                                 : DocumentState::Alive,
                             extras.getDurabilityLevel());
        }
    }

    if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}
