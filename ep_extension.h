/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ep.hh"
#include <memcached/extension.h>

class GetlExtension: public EXTENSION_ASCII_PROTOCOL_DESCRIPTOR {
public:
    GetlExtension(KVStore *kvstore, GET_SERVER_API get_server_api);

    void initialize();

    bool executeGetl(int argc, token_t *argv, void *cookie,
                     bool (*response_handler)(const void*, int, const char*));

private:
    SERVER_HANDLE_V1 *serverApi;
    KVStore *backend;
};
