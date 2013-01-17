/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#ifndef SRC_EP_EXTENSION_H_
#define SRC_EP_EXTENSION_H_ 1

#include "config.h"

#include <memcached/extension.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ep.h"

extern "C" {
    typedef ENGINE_ERROR_CODE (*RESPONSE_HANDLER_T)(const void *, int , const char *);
}

/**
 * Protocol extensions to support item locking.
 */
class GetlExtension: public EXTENSION_ASCII_PROTOCOL_DESCRIPTOR {
public:
    GetlExtension(EventuallyPersistentStore *kvstore, GET_SERVER_API get_server_api);

    void initialize();

    ENGINE_ERROR_CODE executeGetl(int argc, mc_extension_token_t *argv, void *cookie,
                                  RESPONSE_HANDLER_T response_handler);

    ENGINE_ERROR_CODE executeUnl(int argc, mc_extension_token_t *argv, void *cookie,
                                 RESPONSE_HANDLER_T response_handler);


private:
    SERVER_HANDLE_V1 *serverApi;
    EventuallyPersistentStore *backend;
};

#endif  // SRC_EP_EXTENSION_H_

