/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
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

