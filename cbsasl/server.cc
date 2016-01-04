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

#include "cbsasl_internal.h"
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
cbsasl_error_t cbsasl_server_init(const cbsasl_callback_t *,
                                  const char *) {
    if (cb_rand_open(&randgen) != 0) {
        return CBSASL_FAIL;
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
                                 const cbsasl_callback_t*,
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

    *conn = ret;

    (*conn)->mechanism = Mechanism::UNKNOWN;
    return CBSASL_OK;
}

CBSASL_PUBLIC_API
cbsasl_error_t cbsasl_server_start(cbsasl_conn_t** conn,
                                   const char* mech,
                                   const char* clientin,
                                   unsigned int clientinlen,
                                   unsigned char** serverout,
                                   unsigned int* serveroutlen) {

    if (*conn != NULL) {
        cbsasl_dispose(conn);
    }

    cbsasl_error_t err = cbsasl_server_new(nullptr, nullptr, nullptr, nullptr,
                                           nullptr, nullptr, 0, conn);

    if (err != CBSASL_OK) {
        return err;
    }

    auto* server = (*conn)->server.get();

    (*conn)->mechanism = MechanismFactory::toMechanism(mech);
    if ((*conn)->mechanism == Mechanism::UNKNOWN) {
        cbsasl_dispose(conn);
        return CBSASL_BADPARAM;
    }

    server->mech = MechanismFactory::createServerBackend((*conn)->mechanism);
    if (server->mech.get() == nullptr) {
        cbsasl_dispose(conn);
        return CBSASL_NOMEM;
    }

    return server->mech->start(*conn, clientin, clientinlen,
                               (const char**)serverout, serveroutlen);
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
