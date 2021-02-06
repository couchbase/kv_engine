/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
