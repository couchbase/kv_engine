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
#pragma once

#include <cstdint>
#include <string>

namespace cb::mcbp {

enum class DcpStreamEndStatus : uint32_t {
    //! The stream ended due to all items being streamed
    Ok = 0,
    //! The stream closed early due to a close stream message
    Closed = 1,
    //! The stream closed early because the vbucket state changed
    StateChanged = 2,
    //! The stream closed early because the connection was disconnected
    Disconnected = 3,
    //! The stream was closed early because it was too slow
    Slow = 4,
    //! The stream closed early due to backfill failure
    BackfillFail = 5,
    //! The stream closed early because the vbucket is rolling back and
    //! downstream needs to reopen the stream and rollback too
    Rollback = 6,
    //! All filtered collections have been removed so no more data can be sent.
    FilterEmpty = 7,
    //! Lost privileges that were present when stream was created
    LostPrivileges = 8
};

std::string to_string(DcpStreamEndStatus status);

template <typename BasicJsonType>
void to_json(BasicJsonType& j, DcpStreamEndStatus status) {
    j = to_string(status);
}

} // end namespace cb::mcbp
