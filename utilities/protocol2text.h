/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef PROTOCOL2TEXT_H
#define PROTOCOL2TEXT_H

#include <memcached/visibility.h>

#ifdef __cplusplus
extern "C" {
#endif
    MEMCACHED_PUBLIC_API
    const char *memcached_opcode_2_text(uint8_t opcode);
    MEMCACHED_PUBLIC_API
    uint8_t memcached_text_2_opcode(const char *txt);
    MEMCACHED_PUBLIC_API
    const char *memcached_status_2_text(protocol_binary_response_status status);

#ifdef __cplusplus
}
#endif

#endif
