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
#include "cbsasl/cbsasl_internal.h"

#include <atomic>
#include <iostream>

#ifdef DEBUG
std::string to_string(const cbsasl_loglevel_t& level) {
    switch (level) {
    case cbsasl_loglevel_t::None :
        return "None";
    case cbsasl_loglevel_t::Error:
        return "Error";
    case cbsasl_loglevel_t::Fail:
        return "Fail";
    case cbsasl_loglevel_t::Warning:
        return "Warning";
    case cbsasl_loglevel_t::Notice:
        return "Notice";
    case cbsasl_loglevel_t::Debug:
        return "Debug";
    case cbsasl_loglevel_t::Trace:
        return "Trace";
    case cbsasl_loglevel_t::Password:
        return "Password";
    }
}
#endif

static int dummy_log(void* context, int level, const char* message) {
#ifdef DEBUG
    std::cout << to_string((cbsasl_loglevel_t)level) << ": " << message
              << std::endl;
#endif
    return CBSASL_OK;
}

std::atomic<cbsasl_log_fn> global_log(dummy_log);
std::atomic<void*> global_context(nullptr);
std::atomic<cbsasl_loglevel_t> global_level(cbsasl_loglevel_t::Error);


void cbsasl_log(cbsasl_conn_t* connection,
                cbsasl_loglevel_t level,
                const std::string& message) {
    // Internally we use an enum class to make sure that we don't use
    // illegal values.. they are a 1:1 mapping to the #defines
    int lvl = static_cast<int>(level);
    const char* msg = message.c_str();

    if (connection == nullptr || connection->log_fn == nullptr) {
        if (level <= global_level.load()) {
            global_log.load()(global_context.load(), lvl, msg);
        }
    } else {
        if (level <= connection->log_level) {
            connection->log_fn(connection->log_ctx, lvl, msg);
        }
    }
}

void cbsasl_set_default_logger(cbsasl_log_fn log_fn, void *context) {
    global_log.store(log_fn);
    global_context.store(context);
}
