/*
 *     Copyright 2021 Couchbase, Inc.
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
#include "session_validated_command_context.h"
#include "engine_wrapper.h"

#include <daemon/cookie.h>
#include <daemon/mcbp.h>

#include "../../session_cas.h"

SessionValidatedCommandContext::SessionValidatedCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      valid(session_cas.increment_session_counter(
              cookie.getRequest().getCas())) {
}

SessionValidatedCommandContext::~SessionValidatedCommandContext() {
    if (valid) {
        session_cas.decrement_session_counter();
    }
}
ENGINE_ERROR_CODE SessionValidatedCommandContext::step() {
    if (!valid) {
        return ENGINE_KEY_EEXISTS;
    }

    const auto ret = bucket_unknown_command(cookie, mcbpResponseHandlerFn);
    if (ret == ENGINE_SUCCESS) {
        // Send the status back to the caller!
        cookie.setCas(cookie.getRequest().getCas());
        cookie.sendResponse(cb::engine_errc(ret));
    }
    return ret;
}
