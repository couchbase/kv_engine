/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef MEMCACHED_DEFAULT_ENGINE_H
#define MEMCACHED_DEFAULT_ENGINE_H

#include "config.h"

#ifdef __cplusplus
extern "C" {
#endif

    MEMCACHED_PUBLIC_API
    ENGINE_ERROR_CODE create_instance(uint64_t interface,
                                      GET_SERVER_API get_server_api,
                                      ENGINE_HANDLE **handle);

#ifdef __cplusplus
}
#endif

#endif
