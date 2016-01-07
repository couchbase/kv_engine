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

void cbsasl_set_log_level(cbsasl_conn_t* connection,
                          cbsasl_getopt_fn getopt_fn,
                          void* context) {
    const char* result = nullptr;
    unsigned int result_len;
    bool failure = true;
    cbsasl_loglevel_t level;

    if (getopt_fn(context, nullptr, "log level", &result,
                  &result_len) == CBSASL_OK) {
        if (result != nullptr) {
            std::string val(result, result_len);
            try {
                failure = false;
                switch (std::stoul(val)) {
                case CBSASL_LOG_NONE:
                    level = cbsasl_loglevel_t::None;
                    break;
                case CBSASL_LOG_ERR:
                    level = cbsasl_loglevel_t::Error;
                    break;
                case CBSASL_LOG_FAIL:
                    level = cbsasl_loglevel_t::Fail;
                    break;
                case CBSASL_LOG_WARN:
                    level = cbsasl_loglevel_t::Warning;
                    break;
                case CBSASL_LOG_NOTE:
                    level = cbsasl_loglevel_t::Notice;
                    break;
                case CBSASL_LOG_DEBUG:
                    level = cbsasl_loglevel_t::Debug;
                    break;
                case CBSASL_LOG_TRACE:
                    level = cbsasl_loglevel_t::Trace;
                    break;
                case CBSASL_LOG_PASS:
                    level = cbsasl_loglevel_t::Password;
                    break;
                default:
                    failure = true;
                }
            } catch (...) {
                failure = true;
            }
        }
    }

    if (!failure) {
        if (connection == nullptr) {
            global_level.store(level);
        } else {
            connection->log_level = level;
        }
    }
}

cbsasl_loglevel_t cbsasl_get_loglevel(const cbsasl_conn_t* connection) {
    if (connection == nullptr || connection->log_fn == nullptr) {
        return global_level.load();
    } else {
        return connection->log_level;
    }
}
