/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "timings.h"
#include <memcached/protocol_binary.h>
#include <stdlib.h>
#include <string.h>
#include <sstream>

#ifdef BUILD_MCTIMINGS

#ifdef HAVE_ATOMIC
#include <atomic>
#else
#include <cstdatomic>
#endif

typedef struct timings_st {
    /* We collect timings for <=1 us */
    std::atomic<uint32_t> ns;

    /* We collect timings per 10usec */
    std::atomic<uint32_t> usec[100];

    /* we collect timings from 0-49 ms (entry 0 is never used!) */
    std::atomic<uint32_t> msec[50];

    std::atomic<uint32_t> halfsec[10];

    std::atomic<uint32_t> wayout;
} timings_t;

timings_t timings[0x100];

void collect_timing(uint8_t cmd, hrtime_t nsec)
{
    timings_t *t = &timings[cmd];
    hrtime_t usec = nsec / 1000;
    hrtime_t msec = usec / 1000;
    hrtime_t hsec = msec / 500;

    if (usec == 0) {
        t->ns++;
    } else if (usec < 1000) {
        t->usec[usec / 10]++;
    } else if (msec < 50) {
        t->msec[msec]++;
    } else if (hsec < 10) {
        t->halfsec[hsec]++;
    } else {
        t->wayout++;
    }
}

void initialize_timings(void)
{
    int ii, jj;
    for (ii = 0; ii < 0x100; ++ii) {
        timings[ii].ns.store(0);
        for (jj = 0; jj < 100; ++jj) {
            timings[ii].usec[jj].store(0);
        }
        for (jj = 0; jj < 50; ++jj) {
            timings[ii].msec[jj].store(0);
        }
        for (jj = 0; jj < 10; ++jj) {
            timings[ii].halfsec[jj].store(0);
        }
        timings[ii].wayout.store(0);
    }
}
#endif

void generate_timings(uint8_t opcode, const void *cookie)
{
    std::stringstream ss;
#ifdef BUILD_MCTIMINGS
    timings_t *t = &timings[opcode];

    ss << "{\"ns\":" << t->ns.load() << ",\"us\":[";
    for (int ii = 0; ii < 99; ++ii) {
        ss << t->usec[ii].load() << ",";
    }
    ss << t->usec[99].load() << "],\"ms\":[";
    for (int ii = 1; ii < 49; ++ii) {
        ss << t->msec[ii].load() << ",";
    }
    ss << t->msec[49].load() << "],\"500ms\":[";
    for (int ii = 0; ii < 9; ++ii) {
        ss << t->halfsec[ii].load() << ",";
    }
    ss << t->halfsec[9].load() << "],\"wayout\":" << t->wayout.load() << "}";
#else
    ss << "{\"error\":\"The server was built without timings support\"}";
#endif
    std::string str = ss.str();

    binary_response_handler(NULL, 0, NULL, 0, str.data(), str.length(),
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS,
                            0, cookie);
}
