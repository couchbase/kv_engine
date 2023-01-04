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

#include "observe_context.h"
#include "engine_wrapper.h"
#include <daemon/cookie.h>

ObserveCommandContext::ObserveCommandContext(Cookie& cookie)
    : SteppableCommandContext(cookie) {
}

cb::engine_errc ObserveCommandContext::step() {
    auto ret = cb::engine_errc::success;
    do {
        switch (state) {
        case State::Initialize:
            ret = initialize();
            break;
        case State::Observe:
            ret = observe();
            break;
        case State::Done:
            cookie.sendResponse(cb::mcbp::Status::Success,
                                {},
                                {},
                                output.str(),
                                cb::mcbp::Datatype::Raw,
                                persist_time_hint);
            return cb::engine_errc::success;
        }
    } while (ret == cb::engine_errc::success);
    return ret;
}

cb::engine_errc ObserveCommandContext::initialize() {
    size_t offset = 0;

    const auto value = cookie.getRequest().getValue();
    const uint8_t* data = value.data();

    while (offset < value.size()) {
        // Each entry is built up by:
        // 2 bytes vb id
        // 2 bytes key length
        // n bytes key

        // Parse a key
        if (value.size() - offset < 4) {
            cookie.setErrorContext("Requires vbid and keylen.");
            return cb::engine_errc::invalid_arguments;
        }

        Vbid vb_id;
        memcpy(&vb_id, data + offset, sizeof(Vbid));
        vb_id = vb_id.ntoh();
        offset += sizeof(Vbid);

        uint16_t keylen;
        memcpy(&keylen, data + offset, sizeof(uint16_t));
        keylen = ntohs(keylen);
        offset += sizeof(uint16_t);

        if (value.size() - offset < keylen) {
            cookie.setErrorContext("Incorrect keylen");
            return cb::engine_errc::invalid_arguments;
        }

        DocKey key{data + offset,
                   keylen,
                   cookie.isCollectionsSupported()
                           ? DocKeyEncodesCollectionId::Yes
                           : DocKeyEncodesCollectionId::No};
        offset += keylen;
        keys.emplace_back(vb_id, std::move(key));
    }

    if (keys.empty()) {
        // It is allowed to run an "observe" without any keys for some
        // reason. For backwards compatibility treat it as a success.
        state = State::Done;
    } else {
        state = State::Observe;
    }
    return cb::engine_errc::success;
}

cb::engine_errc ObserveCommandContext::observe() {
    const auto& [vb_id, key] = keys.front();
    auto keybuf = key.getBuffer();
    auto vb = vb_id.hton().get();
    auto addResult = [this, vb, keybuf](uint8_t st, uint64_t cas) {
        const uint16_t keylen =
                htons(gsl::narrow_cast<uint16_t>(keybuf.size()));
        const auto netcas = htonll(cas);
        output.write(reinterpret_cast<const char*>(&vb), sizeof(vb));
        output.write(reinterpret_cast<const char*>(&keylen), sizeof(keylen));
        output.write(reinterpret_cast<const char*>(keybuf.data()),
                     keybuf.size());
        output.write(reinterpret_cast<const char*>(&st), sizeof(st));
        output.write(reinterpret_cast<const char*>(&netcas), sizeof(cas));
    };

    auto ret = bucket_observe(cookie, key, vb_id, addResult, persist_time_hint);
    if (ret == cb::engine_errc::success) {
        keys.pop_front();
        if (keys.empty()) {
            state = State::Done;
        }
    }

    return ret;
}
