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
#include "engine_wrapper.h"
#include "unlock_context.h"

#include <daemon/memcached.h>

cb::engine_errc UnlockCommandContext::unlock() {
    auto ret = bucket_unlock(cookie, cookie.getRequestKey(), vbucket, cas);
    if (ret == cb::engine_errc::success) {
        update_topkeys(cookie);
        cookie.sendResponse(cb::mcbp::Status::Success);
        state = State::Done;
    }

    return ret;
}

cb::engine_errc UnlockCommandContext::step() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::Unlock:
            ret = unlock();
            break;
        case State::Done:
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);

    return ret;
}
