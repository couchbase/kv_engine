/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef SRC_EP_EXTENSION_H_
#define SRC_EP_EXTENSION_H_ 1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ep.h"
#include <memcached/extension.h>

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

