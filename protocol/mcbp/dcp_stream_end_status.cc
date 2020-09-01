/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc.
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

#include <mcbp/protocol/dcp_stream_end_status.h>
#include <stdexcept>
#include <string>

using namespace cb::mcbp;

std::string cb::mcbp::to_string(DcpStreamEndStatus status) {
    switch (status) {
    case DcpStreamEndStatus::Ok:
        return "The stream ended due to all items being streamed";
    case DcpStreamEndStatus::Closed:
        return "The stream closed early due to a close stream message";
    case DcpStreamEndStatus::StateChanged:
        return "The stream closed early because the vbucket state changed";
    case DcpStreamEndStatus::Disconnected:
        return "The stream closed early because the conn was disconnected";
    case DcpStreamEndStatus::Slow:
        return "The stream was closed early because it was too slow";
    case DcpStreamEndStatus::BackfillFail:
        return "The stream closed early due to backfill failure";
    case DcpStreamEndStatus::Rollback:
        return "The stream closed early because the vbucket rollback'ed";
    case DcpStreamEndStatus::FilterEmpty:
        return "The stream closed because all of the filtered collections "
               "were deleted";
    case DcpStreamEndStatus::LostPrivileges:
        return "The stream closed because required privileges were lost";
    }

    throw std::invalid_argument(
            "to_string(cb::mcbp::DcpStreamEndStatus): unknown status:" +
            std::to_string(uint32_t(status)));
}
