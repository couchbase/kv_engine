/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
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
