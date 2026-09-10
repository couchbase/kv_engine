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
#include "no_success_response_steppable_context.h"
#include <daemon/cookie.h>
#include <memcached/durability_spec.h>
#include <memcached/limits.h>
#include <memcached/protocol_binary.h>
#include <xattr/blob.h>

static cb::engine_errc do_dcp_prepare(Cookie& cookie) {
    const auto& req = cookie.getRequest();
    const auto& extras =
            req.getCommandSpecifics<cb::mcbp::request::DcpPreparePayload>();
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

    return dcpPrepare(
            cookie,
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
            extras.getNru(),
            extras.getDeleted() ? DocumentState::Deleted : DocumentState::Alive,
            extras.getDurabilityLevel());
}

void dcp_prepare_executor(Cookie& cookie) {
    cookie.obtainContext<NoSuccessResponseCommandContext>(
                  cookie, [](Cookie& c) { return do_dcp_prepare(c); })
            .drive();
}
