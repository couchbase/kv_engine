/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc.
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

#include "sasl_refresh_command_context.h"

#include <cbsasl/mechanism.h>
#include <daemon/connection.h>
#include <daemon/cookie.h>
#include <daemon/runtime.h>

static void cbsasl_refresh_main(void* cookie) {
    ENGINE_ERROR_CODE rv = ENGINE_SUCCESS;
    try {
        using namespace cb::sasl;
        server::refresh();
        set_default_bucket_enabled(
                mechanism::plain::authenticate("default", "") == Error::OK);
    } catch (...) {
        rv = ENGINE_FAILED;
    }

    notify_io_complete(cookie, rv);
}

ENGINE_ERROR_CODE SaslRefreshCommandContext::refresh() {
    state = State::Done;

    cb_thread_t tid;
    auto status = cb_create_named_thread(&tid,
                                         cbsasl_refresh_main,
                                         static_cast<void*>(&cookie),
                                         1,
                                         "mc:refresh_sasl");
    if (status != 0) {
        LOG_WARNING("{}: Failed to create cbsasl db update thread: {}",
                    connection.getId(),
                    strerror(status));
        return ENGINE_DISCONNECT;
    }

    return ENGINE_EWOULDBLOCK;
}

void SaslRefreshCommandContext::done() {
    cookie.sendResponse(cb::mcbp::Status::Success);
}
