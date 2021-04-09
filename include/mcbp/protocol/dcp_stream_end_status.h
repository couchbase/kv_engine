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
    //! The stream was closed early because it was too slow (currently unused,
    //! but not deleted because it is part of the externally-visible API)
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

} // end namespace cb::mcbp
