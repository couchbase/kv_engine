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

#include "seqno_persistence_context.h"
#include "engine_wrapper.h"
#include <daemon/cookie.h>

static uint64_t getSeqno(cb::const_byte_buffer extdata) {
    return ntohll(*reinterpret_cast<const uint64_t*>(extdata.data()));
}

SeqnoPersistenceCommandContext::SeqnoPersistenceCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie),
      seqno(getSeqno(cookie.getHeader().getExtdata())) {
}

cb::engine_errc SeqnoPersistenceCommandContext::step() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::Wait:
            ret = bucket_wait_for_seqno_persistence(
                    cookie, seqno, cookie.getRequest().getVBucket());
            if (ret == cb::engine_errc::success) {
                state = State::Done;
            }
            break;
        case State::Done:
            cookie.sendResponse(cb::engine_errc::success);
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);
    return ret;
}
