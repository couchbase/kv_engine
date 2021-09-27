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
#include "dcp_system_event_executor.h"
#include "engine_wrapper.h"
#include "executors.h"
#include "utilities.h"

#include <memcached/protocol_binary.h>

void dcp_system_event_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        using cb::mcbp::request::DcpSystemEventPayload;
        const auto& request = cookie.getRequest();
        const auto& payload =
                request.getCommandSpecifics<DcpSystemEventPayload>();

        ret = dcpSystemEvent(cookie,
                             request.getOpaque(),
                             request.getVBucket(),
                             mcbp::systemevent::id(payload.getEvent()),
                             payload.getBySeqno(),
                             mcbp::systemevent::version(payload.getVersion()),
                             request.getKey(),
                             request.getValue());
    }

    if (ret != cb::engine_errc::success) {
        handle_executor_status(cookie, ret);
    }
}
