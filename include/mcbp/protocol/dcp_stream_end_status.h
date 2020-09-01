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
