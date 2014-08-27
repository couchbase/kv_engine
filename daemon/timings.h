/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#ifndef TIMINIGS_H
#define TIMINIGS_H

#include <platform/platform.h>

#ifdef __cplusplus
extern "C" {
#endif

#if defined(HAVE_ATOMIC) || defined(HAVE_CSTDATOMIC)
#define BUILD_MCTIMINGS 1
#endif

#ifdef BUILD_MCTIMINGS
    void collect_timing(uint8_t cmd, hrtime_t delay);
    void initialize_timings(void);
#else

#define collect_timing(a, b)
#define initialize_timings()

#endif
    void generate_timings(uint8_t opcode, const void *cookie);

    bool binary_response_handler(const void *key, uint16_t keylen,
                                 const void *ext, uint8_t extlen,
                                 const void *body, uint32_t bodylen,
                                 uint8_t datatype, uint16_t status,
                                 uint64_t cas, const void *cookie);
#ifdef __cplusplus
}
#endif

#endif
