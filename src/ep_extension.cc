/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 NorthScale, Inc.
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
#include "config.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ep_extension.h"
#include <memcached/extension.h>

#define ITEM_LOCK_TIMEOUT       15    /* 15 seconds */
#define MAX_KEY_LEN             250   /* maximum permissible key length */


static GetlExtension* getExtension(const void* cookie)
{
    return reinterpret_cast<GetlExtension*>(const_cast<void *>(cookie));
}

extern "C" {

    static const char *ext_get_name(const void *cmd_cookie) {
        (void) cmd_cookie;
        return "getl";
    }

    static ENGINE_ERROR_CODE ext_execute(const void *cmd_cookie, const void *cookie,
            int argc, mc_extension_token_t *argv,
            RESPONSE_HANDLER_T response_handler) {
        (void) cmd_cookie;

        if (strncmp(argv[0].value, "getl", argv[0].length) == 0) {
            return getExtension(cmd_cookie)->executeGetl(argc, argv,
                                                         (void *)cookie,
                                                         response_handler);
        } else {
            return getExtension(cmd_cookie)->executeUnl(argc, argv,
                                                        (void *)cookie,
                                                        response_handler);
        }
    }

    static bool ext_accept(const void *cmd_cookie, void *cookie,
            int argc, mc_extension_token_t *argv, size_t *ndata,
            char **ptr) {
        (void) cmd_cookie;
        (void) cookie;
        (void) argc;
        (void) ndata;
        (void) ptr;
        // accept both getl (get locked) and unl (unlock)commands
        return argc >= 1 && (strncmp(argv[0].value, "getl", argv[0].length) == 0 ||
                             strncmp(argv[0].value, "unl", argv[0].length) == 0);
    }

    static void ext_abort(const void *cmd_cookie, const void *cookie) {
        (void) cmd_cookie;
        (void) cookie;
    }

}  /* extern C */

GetlExtension::GetlExtension(EventuallyPersistentStore *kvstore,
                             GET_SERVER_API get_server_api):
    backend(kvstore)
{
    serverApi = get_server_api();
}

void GetlExtension::initialize()
{
    if (serverApi != NULL) {
        EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *ptr = this;
        get_name = ext_get_name;
        accept = ext_accept;
        execute = ext_execute;
        abort = ext_abort;
        cookie = ptr;
        serverApi->extension->register_extension(EXTENSION_ASCII_PROTOCOL, ptr);

        getLogger()->log(EXTENSION_LOG_INFO, NULL, "Loaded extension: getl\n");
    }
}

ENGINE_ERROR_CODE GetlExtension::executeGetl(int argc, mc_extension_token_t *argv,
                                             void *response_cookie,
                                             RESPONSE_HANDLER_T response_handler)
{
    uint32_t lockTimeout = ITEM_LOCK_TIMEOUT;

    if (argc == 3) {
        if (!parseUint32(argv[2].value, &lockTimeout) ||
                lockTimeout > (ITEM_LOCK_TIMEOUT * 2)) {
            lockTimeout = ITEM_LOCK_TIMEOUT;
        }
    } else if (argc != 2) {
        return response_handler(response_cookie,
                                sizeof("CLIENT_ERROR\r\n") - 1,
                                "CLIENT_ERROR\r\n");
    }

    std::string k(argv[1].value, argv[1].length);
    RememberingCallback<GetValue> getCb;

    // TODO:  Get vbucket ID here.
    bool gotLock = backend->getLocked(k, 0, getCb,
            serverApi->core->get_current_time(),
            lockTimeout, response_cookie);

    Item *itm = NULL;
    ENGINE_ERROR_CODE ret;

    getCb.waitForValue();

    ENGINE_ERROR_CODE rv = getCb.val.getStatus();
    if (rv == ENGINE_SUCCESS) {
        itm = getCb.val.getValue();
        std::stringstream strm;

        strm << "VALUE " << itm->getKey() << " " << ntohl(itm->getFlags())
             << " " << itm->getNBytes() << " " << itm->getCas() << "\r\n";

        std::string strVal = strm.str();
        size_t len = strVal.length();

        if ((response_handler(response_cookie, static_cast<int>(len),
                              strVal.c_str()) == ENGINE_SUCCESS) &&
            (response_handler(response_cookie, itm->getNBytes(),
                              itm->getData()) == ENGINE_SUCCESS) &&
            (response_handler(response_cookie, 7,
                              "\r\nEND\r\n") == ENGINE_SUCCESS)) {
            ret = ENGINE_SUCCESS;
        } else {
            ret = ENGINE_DISCONNECT;
        }
    } else if (rv == ENGINE_EWOULDBLOCK) {
        ret = rv;
    } else if (!gotLock) {
        ret = response_handler(response_cookie,
                               sizeof("LOCK_ERROR\r\n") - 1, "LOCK_ERROR\r\n");
    } else {
        ret = response_handler(response_cookie,
                               sizeof("NOT_FOUND\r\n") - 1, "NOT_FOUND\r\n");
    }

    if (itm != NULL) {
        delete itm;
    }

    return ret;
}

ENGINE_ERROR_CODE GetlExtension::executeUnl(int argc, mc_extension_token_t *argv,
                                            void *response_cookie,
                                            RESPONSE_HANDLER_T response_handler)
{
    uint64_t cas = 0;

    // we need a valid cas value
    if (argc != 3 || !parseUint64(argv[2].value, &cas)) {
        return response_handler(response_cookie,
                                sizeof("CLIENT_ERROR\r\n") - 1,
                                "CLIENT_ERROR\r\n");
    }

    std::string k(argv[1].value, argv[1].length);
    RememberingCallback<GetValue> getCb;

    ENGINE_ERROR_CODE rv = backend->unlockKey(k, 0, cas, serverApi->core->get_current_time());

    if (rv == ENGINE_SUCCESS) {
        return response_handler(response_cookie,
                                sizeof("UNLOCKED\r\n") -1, "UNLOCKED\r\n");

    } else if (rv == ENGINE_TMPFAIL) {
        return response_handler(response_cookie,
                                sizeof("UNLOCK_ERROR\r\n") - 1, "UNLOCK_ERROR\r\n");
    } else {
        return response_handler(response_cookie,
                                sizeof("NOT_FOUND\r\n") - 1, "NOT_FOUND\r\n");
    }
}
