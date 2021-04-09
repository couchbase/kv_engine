/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "executors.h"

#include "engine_wrapper.h"
#include <daemon/cookie.h>
#include <mcbp/protocol/header.h>
#include <mcbp/protocol/request.h>

void dcp_close_stream_executor(Cookie& cookie) {
    auto ret = cookie.swapAiostat(cb::engine_errc::success);

    if (ret == cb::engine_errc::success) {
        const auto& header = cookie.getHeader().getRequest();
        cb::mcbp::DcpStreamId dcpStreamId; // Initialises to 'none'
        header.parseFrameExtras([&dcpStreamId](
                                        cb::mcbp::request::FrameInfoId id,
                                        cb::const_byte_buffer data) -> bool {
            if (id == cb::mcbp::request::FrameInfoId::DcpStreamId) {
                // Data is the u16 stream-ID in network byte-order
                dcpStreamId = cb::mcbp::DcpStreamId(
                        ntohs(*reinterpret_cast<const uint16_t*>(data.data())));
                return false;
            }
            return true;
        });

        ret = dcpCloseStream(
                cookie, header.getOpaque(), header.getVBucket(), dcpStreamId);
    }

    handle_executor_status(cookie, ret);
}

