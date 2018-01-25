/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#include <memcached/extension.h>
#include <memcached/server_api.h>
#include <memcached/types.h>
#include <memcached/visibility.h>

extern "C" {
MEMCACHED_PUBLIC_API
EXTENSION_ERROR_CODE memcached_extensions_initialize(
        const char* config, GET_SERVER_API get_server_api);
}

