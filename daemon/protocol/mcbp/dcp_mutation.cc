/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "dcp_mutation.h"
#include "engine_wrapper.h"
#include "executors.h"
#include "utilities.h"

#include <memcached/limits.h>
#include <memcached/protocol_binary.h>
#include <xattr/blob.h>
#include <xattr/utils.h>

static inline cb::engine_errc do_dcp_mutation(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    const auto extdata = req.getExtdata();
    const auto& extras =
            *reinterpret_cast<const cb::mcbp::request::DcpMutationPayload*>(
                    extdata.data());
    const auto datatype = uint8_t(req.getDatatype());
    const auto nmeta = extras.getNmeta();
    auto value = req.getValue();
    const cb::const_byte_buffer meta = {value.data() + value.size() - nmeta,
                                        nmeta};
    value = {value.data(), value.size() - nmeta};

    uint32_t priv_bytes = 0;
    if (mcbp::datatype::is_xattr(datatype)) {
        const char* payload = reinterpret_cast<const char*>(value.data());
        cb::xattr::Blob blob({const_cast<char*>(payload), value.size()},
                             mcbp::datatype::is_snappy(datatype));
        priv_bytes = uint32_t(blob.get_system_size());
        if (priv_bytes > cb::limits::PrivilegedBytes) {
            return cb::engine_errc::too_big;
        }
    }

    return dcpMutation(cookie,
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
                       meta,
                       extras.getNru());
}

void dcp_mutation_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        ret = do_dcp_mutation(cookie);
    }

    if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}
