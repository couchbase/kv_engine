/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef PROTOCOL2TEXT_H
#define PROTOCOL2TEXT_H

#ifdef __cplusplus
extern "C" {
#endif
    const char *memcached_opcode_2_text(uint8_t opcode);
    uint8_t memcached_text_2_opcode(const char *txt);
#ifdef __cplusplus
}
#endif

#endif
