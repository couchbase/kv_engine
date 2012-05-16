/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/* This is a small little mock engine used for testing of the tap code.. */
#ifndef MEMCACHED_TAP_MOCK_ENGINE_H
#define MEMCACHED_TAP_MOCK_ENGINE_H

#include "config.h"

#include <memcached/engine.h>

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
