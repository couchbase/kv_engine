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
#include "unlock_context.h"

#include <daemon/debug_helpers.h>
#include <daemon/mcbp.h>

ENGINE_ERROR_CODE UnlockCommandContext::initialize() {
    if (settings.getVerbose() > 1) {
        char buffer[1024];
        if (key_to_printable_buffer(buffer, sizeof(buffer),
                                    connection.getId(), true,
                                    "UNLOCK",
                                    reinterpret_cast<const char*>(key.data()),
                                    key.size()) != -1) {
            LOG_DEBUG(&connection, "%s", buffer);
        }
    }
    state = State::Unlock;
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE UnlockCommandContext::unlock() {
    auto ret = bucket_unlock(connection, key, vbucket, cas);
    if (ret == ENGINE_SUCCESS) {
        update_topkeys(key, &connection);
        mcbp_write_packet(&connection, PROTOCOL_BINARY_RESPONSE_SUCCESS);
        state = State::Done;
    }

    return ret;
}

ENGINE_ERROR_CODE UnlockCommandContext::step() {
    ENGINE_ERROR_CODE ret;
    do {
        switch (state) {
        case State::Initialize:
            ret = initialize();
            break;
        case State::Unlock:
            ret = unlock();
            break;
        case State::Done:
            return ENGINE_SUCCESS;
        }
    } while (ret == ENGINE_SUCCESS);

    return ret;
}
