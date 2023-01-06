/*
 *     Copyright 2023 Couchbase, Inc
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
#include "single_state_steppable_context.h"
#include <daemon/cookie.h>

SingleStateCommandContext::SingleStateCommandContext(
        Cookie& cookie, std::function<cb::engine_errc(Cookie&)> handler)
    : SteppableCommandContext(cookie), handler(std::move(handler)) {
}

cb::engine_errc SingleStateCommandContext::step() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::Wait:
            ret = handler(cookie);
            if (ret == cb::engine_errc::success) {
                state = State::Done;
            }
            break;
        case State::Done:
            cookie.sendResponse(
                    cb::engine_errc::success, {}, cookie.getErrorContext());
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);
    return ret;
}
