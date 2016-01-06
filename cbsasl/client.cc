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

#include "cbsasl/cbsasl.h"
#include "cbsasl/cbsasl_internal.h"

#include "util.h"
#include "mechanismfactory.h"
#include <time.h>
#include <stdlib.h>
#include <string.h>

CBSASL_PUBLIC_API
cbsasl_error_t cbsasl_client_new(const char*,
                                 const char*,
                                 const char*,
                                 const char*,
                                 const cbsasl_callback_t* prompt_supp,
                                 unsigned,
                                 cbsasl_conn_t** pconn) {
    cbsasl_callback_t* callbacks = (cbsasl_callback_t*)prompt_supp;

    if (prompt_supp == nullptr || pconn == nullptr) {
        return CBSASL_BADPARAM;
    }

    cbsasl_conn_t* conn = nullptr;
    try {
        conn = new cbsasl_conn_t;
        conn->client.reset(new ClientConnection);
    } catch (std::bad_alloc&) {
        delete conn;
        *pconn = nullptr;
        return CBSASL_NOMEM;
    }

    int ii = 0;
    /* Locate the callbacks */
    while (callbacks[ii].id != CBSASL_CB_LIST_END) {
        union {
            cbsasl_get_authname_fn get_authname_fn;
            cbsasl_get_username_fn get_username_fn;
            cbsasl_get_password_fn get_password_fn;
            cbsasl_log_fn log_fn;
            cbsasl_get_cnonce_fn get_cnonce_fn;
            int (* proc)(void);
        } hack;
        hack.proc = callbacks[ii].proc;

        switch (callbacks[ii].id) {
        case CBSASL_CB_USER:
        case CBSASL_CB_AUTHNAME:
            conn->client->get_username = hack.get_username_fn;
            conn->client->get_username_ctx = callbacks[ii].context;
            break;
        case CBSASL_CB_PASS:
            conn->client->get_password = hack.get_password_fn;
            conn->client->get_password_ctx = callbacks[ii].context;
            break;
        case CBSASL_CB_LOG:
            conn->log_fn = hack.log_fn;
            conn->log_ctx = callbacks[ii].context;
            break;
        case CBSASL_CB_CNONCE:
            conn->get_cnonce_fn = hack.get_cnonce_fn;
            conn->get_cnonce_ctx = callbacks[ii].context;
        default:
            /* Ignore unknown */
            ;
        }
        ++ii;
    }

    if (conn->client->get_username == nullptr ||
        conn->client->get_password == nullptr) {
        cbsasl_dispose(&conn);
        return CBSASL_NOUSER;
    }

    *pconn = conn;

    return CBSASL_OK;
}

CBSASL_PUBLIC_API
cbsasl_error_t cbsasl_client_start(cbsasl_conn_t* conn,
                                   const char* mechlist,
                                   void**,
                                   const char** clientout,
                                   unsigned int* clientoutlen,
                                   const char** mech) {
    ClientConnection* client;

    if (conn == nullptr || (client = conn->client.get()) == nullptr) {
        return CBSASL_BADPARAM;
    }

    conn->mechanism = MechanismFactory::selectMechanism(mechlist);
    if (conn->mechanism == Mechanism::UNKNOWN) {
        return CBSASL_NOMECH;
    }

    client->mech = MechanismFactory::createClientBackend(conn->mechanism);
    if (client->mech.get() == nullptr) {
        return CBSASL_NOMEM;
    }

    if (mech != nullptr) {
        *mech = client->mech->getName().c_str();
    }

    return client->mech->start(conn, nullptr, 0, clientout, clientoutlen);
}

CBSASL_PUBLIC_API
cbsasl_error_t cbsasl_client_step(cbsasl_conn_t* conn,
                                  const char* serverin,
                                  unsigned int serverinlen,
                                  void**,
                                  const char** clientout,
                                  unsigned int* clientoutlen) {
    ClientConnection* client;

    if (conn == nullptr || (client = conn->client.get()) == nullptr) {
        return CBSASL_BADPARAM;
    }

    return client->mech->step(conn, serverin, serverinlen, clientout,
                              clientoutlen);
}

cbsasl_error_t cbsasl_get_username(cbsasl_get_username_fn function, void* context,
                                   const char** username,
                                   unsigned int* usernamelen) {
    if (function(context, CBSASL_CB_USER, username, usernamelen) != 0) {
        return CBSASL_FAIL;
    }
    return CBSASL_OK;
}

cbsasl_error_t cbsasl_get_password(cbsasl_get_password_fn function,
                                   cbsasl_conn_t* conn,
                                   void* context,
                                   cbsasl_secret_t** psecret) {
    if (function(conn, context, CBSASL_CB_PASS, psecret) != 0) {
        return CBSASL_FAIL;
    }
    return CBSASL_OK;
}
