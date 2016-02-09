/*
 *     Copyright 2013 Couchbase, Inc.
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

#include <cbsasl/cbsasl.h>
#include "cbsasl/cbsasl_internal.h"

#include "mechanismfactory.h"
#include "pwfile.h"
#include "util.h"
#include <memory.h>
#include <platform/random.h>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <time.h>

static cb_rand_t randgen;

CBSASL_PUBLIC_API
cbsasl_error_t cbsasl_listmech(cbsasl_conn_t* conn,
                               const char* user,
                               const char* prefix,
                               const char* sep,
                               const char* suffix,
                               const char** result,
                               unsigned* len,
                               int* count) {
    return MechanismFactory::list(conn, user, prefix, sep, suffix, result, len,
                                  count);
}

CBSASL_PUBLIC_API
cbsasl_error_t cbsasl_server_init(const cbsasl_callback_t* callbacks,
                                  const char*) {
    if (cb_rand_open(&randgen) != 0) {
        return CBSASL_FAIL;
    }

    if (callbacks != nullptr) {
        cbsasl_getopt_fn getopt_fn = nullptr;
        void* getopt_ctx = nullptr;
        int ii = 0;
        while (callbacks[ii].id != CBSASL_CB_LIST_END) {
            union {
                cbsasl_log_fn log_fn;
                cbsasl_getopt_fn getopt;

                int (* proc)(void);
            } hack;
            hack.proc = callbacks[ii].proc;

            switch (callbacks[ii].id) {
            case CBSASL_CB_LOG:
                cbsasl_set_default_logger(hack.log_fn, callbacks[ii].context);
                break;
            case CBSASL_CB_GETOPT:
                getopt_fn = hack.getopt;
                getopt_ctx = callbacks[ii].context;
                break;
            default:
                /* Ignore unknown */
                ;
            }
            ++ii;
        }

        if (getopt_fn != nullptr) {
            cbsasl_set_log_level(nullptr, getopt_fn, getopt_ctx);
            cbsasl_set_hmac_iteration_count(getopt_fn, getopt_ctx);
            cbsasl_set_available_mechanisms(getopt_fn, getopt_ctx);
        }

    }
    return load_user_db();
}

CBSASL_PUBLIC_API
cbsasl_error_t cbsasl_server_term(void) {
    return cb_rand_close(randgen) == 0 ? CBSASL_OK : CBSASL_FAIL;
}

CBSASL_PUBLIC_API
cbsasl_error_t cbsasl_server_new(const char*,
                                 const char*,
                                 const char*,
                                 const char*,
                                 const char*,
                                 const cbsasl_callback_t* callbacks,
                                 unsigned int,
                                 cbsasl_conn_t** conn) {
    if (conn == nullptr) {
        return CBSASL_BADPARAM;
    }

    cbsasl_conn_t* ret = nullptr;
    try {
        ret = new cbsasl_conn_t;
        ret->server.reset(new ServerConnection);
    } catch (std::bad_alloc&) {
        delete *conn;
        *conn = nullptr;
        return CBSASL_NOMEM;
    }

    if (callbacks != nullptr) {
        int ii = 0;
        while (callbacks[ii].id != CBSASL_CB_LIST_END) {
            union {
                cbsasl_log_fn log_fn;
                cbsasl_get_cnonce_fn get_cnonce_fn;
                cbsasl_getopt_fn getopt_fn;

                int (* proc)(void);
            } hack;
            hack.proc = callbacks[ii].proc;

            switch (callbacks[ii].id) {
            case CBSASL_CB_LOG:
                ret->log_fn = hack.log_fn;
                ret->log_ctx = callbacks[ii].context;
                break;
            case CBSASL_CB_CNONCE:
                ret->get_cnonce_fn = hack.get_cnonce_fn;
                ret->get_cnonce_ctx = callbacks[ii].context;
                break;
            case CBSASL_CB_GETOPT:
                ret->getopt_fn = hack.getopt_fn;
                ret->getopt_ctx = callbacks[ii].context;
                break;

            default:
                /* Ignore unknown */
                ;
            }
            ++ii;
        }

        if (ret->getopt_fn != nullptr) {
            cbsasl_set_log_level(ret, ret->getopt_fn, ret->getopt_ctx);
        }
    }

    *conn = ret;

    (*conn)->mechanism = Mechanism::UNKNOWN;
    return CBSASL_OK;
}

CBSASL_PUBLIC_API
cbsasl_error_t cbsasl_server_start(cbsasl_conn_t* conn,
                                   const char* mech,
                                   const char* clientin,
                                   unsigned int clientinlen,
                                   const char** serverout,
                                   unsigned int* serveroutlen) {
    if (conn == nullptr) {
        return CBSASL_BADPARAM;
    }

    auto* server = conn->server.get();

    conn->mechanism = MechanismFactory::toMechanism(mech);
    if (conn->mechanism == Mechanism::UNKNOWN) {
        cbsasl_log(conn, cbsasl_loglevel_t::Error,
                   "Failed to look up mechanism [" + std::string(mech) + "]");
        return CBSASL_NOMECH;
    }

    cbsasl_log(conn, cbsasl_loglevel_t::Debug, "Client requests the use of [" +
                                               MechanismFactory::toString(
                                                   conn->mechanism) + "]");

    server->mech = MechanismFactory::createServerBackend(conn->mechanism);
    if (server->mech.get() == nullptr) {
        cbsasl_log(conn, cbsasl_loglevel_t::Error,
                   "Failed to create instance of [" +
                   MechanismFactory::toString(conn->mechanism) + "]");
        return CBSASL_NOMEM;
    }

    return server->mech->start(conn, clientin, clientinlen,
                               serverout, serveroutlen);
}

CBSASL_PUBLIC_API
cbsasl_error_t cbsasl_server_step(cbsasl_conn_t* conn,
                                  const char* input,
                                  unsigned inputlen,
                                  const char** output,
                                  unsigned* outputlen) {
    if (conn == NULL || conn->server.get() == nullptr) {
        return CBSASL_BADPARAM;
    }
    return conn->server->mech->step(conn, input, inputlen, output, outputlen);
}

CBSASL_PUBLIC_API
cbsasl_error_t cbsasl_server_refresh(void) {
    return load_user_db();
}

CBSASL_PUBLIC_API
cbsasl_error_t cbsasl_getprop(cbsasl_conn_t* conn,
                              cbsasl_prop_t propnum,
                              const void** pvalue) {
    if (conn == NULL || conn->server.get() == nullptr || pvalue == NULL) {
        return CBSASL_BADPARAM;
    }

    switch (propnum) {
    case CBSASL_USERNAME:
        *pvalue = conn->server->username.c_str();
        break;
    default:
        return CBSASL_BADPARAM;
    }

    return CBSASL_OK;
}

/* This function is added to keep the randgen static ;-) */
cbsasl_error_t cbsasl_secure_random(char* dest, size_t len) {
    return (cb_rand_get(randgen, dest, len) == 0) ? CBSASL_OK : CBSASL_FAIL;
}
