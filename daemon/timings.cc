/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "timings.h"
#include "settings.h"
#include "utilities/protocol2text.h"
#include <memcached/protocol_binary.h>
#include <stdlib.h>
#include <string.h>
#include <sstream>

#ifdef HAVE_ATOMIC
#include <atomic>
#else
#include <cstdatomic>
#endif

extern "C" {
    extern struct settings settings;
}


typedef struct timings_st {
    /* We collect timings for <=1 us */
    std::atomic<uint32_t> ns;

    /* We collect timings per 10usec */
    std::atomic<uint32_t> usec[100];

    /* we collect timings from 0-49 ms (entry 0 is never used!) */
    std::atomic<uint32_t> msec[50];

    std::atomic<uint32_t> halfsec[10];

    std::atomic<uint32_t> wayout;

    std::atomic<uint64_t> total;
} timings_t;

timings_t timings[0x100];

void collect_timing(SOCKET sfd,
                    const char *peername,
                    const char *sockname,
                    uint8_t cmd,
                    hrtime_t nsec)
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

    t->total++;

    if (hsec > 1) {
        const char *opcode = memcached_opcode_2_text(cmd);
        char opcodetext[10];
        if (opcode == NULL) {
            snprintf(opcodetext, sizeof(opcodetext), "0x%0X", cmd);
            opcode = opcodetext;
        }
        if (peername == NULL) {
            peername = "unknown";
        }
        if (sockname == NULL) {
            sockname = "unknown";
        }
        settings.extensions.logger->log(EXTENSION_LOG_WARNING, NULL,
                                        "%u: Slow %s operation on connection (%s => %s): %lu ms",
                                        (unsigned int)sfd,
                                        opcode,
                                        peername, sockname,
                                        (unsigned long)msec);
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
        timings[ii].total.store(0);
    }
}

void generate_timings(uint8_t opcode, const void *cookie)
{
    std::stringstream ss;
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
    std::string str = ss.str();

    binary_response_handler(NULL, 0, NULL, 0, str.data(),
                            uint32_t(str.length()),
                            PROTOCOL_BINARY_RAW_BYTES,
                            PROTOCOL_BINARY_RESPONSE_SUCCESS,
                            0, cookie);
}


uint64_t get_aggregated_cmd_stats(cmd_stat_t type)
{
    uint64_t ret = 0;
    static uint8_t mutations[] = {
        PROTOCOL_BINARY_CMD_ADD,
        PROTOCOL_BINARY_CMD_ADDQ,
        PROTOCOL_BINARY_CMD_APPEND,
        PROTOCOL_BINARY_CMD_APPENDQ,
        PROTOCOL_BINARY_CMD_DECREMENT,
        PROTOCOL_BINARY_CMD_DECREMENTQ,
        PROTOCOL_BINARY_CMD_DELETE,
        PROTOCOL_BINARY_CMD_DELETEQ,
        PROTOCOL_BINARY_CMD_GAT,
        PROTOCOL_BINARY_CMD_GATQ,
        PROTOCOL_BINARY_CMD_INCREMENT,
        PROTOCOL_BINARY_CMD_INCREMENTQ,
        PROTOCOL_BINARY_CMD_PREPEND,
        PROTOCOL_BINARY_CMD_PREPENDQ,
        PROTOCOL_BINARY_CMD_REPLACE,
        PROTOCOL_BINARY_CMD_REPLACEQ,
        PROTOCOL_BINARY_CMD_SET,
        PROTOCOL_BINARY_CMD_SETQ,
        PROTOCOL_BINARY_CMD_TOUCH,
        PROTOCOL_BINARY_CMD_INVALID};
    static uint8_t retrival[] = {
        PROTOCOL_BINARY_CMD_GAT,
        PROTOCOL_BINARY_CMD_GATQ,
        PROTOCOL_BINARY_CMD_GET,
        PROTOCOL_BINARY_CMD_GETK,
        PROTOCOL_BINARY_CMD_GETKQ,
        PROTOCOL_BINARY_CMD_GETQ,
        PROTOCOL_BINARY_CMD_GET_LOCKED,
        PROTOCOL_BINARY_CMD_GET_RANDOM_KEY,
        PROTOCOL_BINARY_CMD_GET_REPLICA,
        PROTOCOL_BINARY_CMD_INVALID };
    static uint8_t total[] = {
        PROTOCOL_BINARY_CMD_ADD,
        PROTOCOL_BINARY_CMD_ADDQ,
        PROTOCOL_BINARY_CMD_APPEND,
        PROTOCOL_BINARY_CMD_APPENDQ,
        PROTOCOL_BINARY_CMD_DECREMENT,
        PROTOCOL_BINARY_CMD_DECREMENTQ,
        PROTOCOL_BINARY_CMD_DELETE,
        PROTOCOL_BINARY_CMD_DELETEQ,
        PROTOCOL_BINARY_CMD_GAT,
        PROTOCOL_BINARY_CMD_GATQ,
        PROTOCOL_BINARY_CMD_GET,
        PROTOCOL_BINARY_CMD_GETK,
        PROTOCOL_BINARY_CMD_GETKQ,
        PROTOCOL_BINARY_CMD_GETQ,
        PROTOCOL_BINARY_CMD_GET_LOCKED,
        PROTOCOL_BINARY_CMD_GET_RANDOM_KEY,
        PROTOCOL_BINARY_CMD_GET_REPLICA,
        PROTOCOL_BINARY_CMD_INCREMENT,
        PROTOCOL_BINARY_CMD_INCREMENTQ,
        PROTOCOL_BINARY_CMD_PREPEND,
        PROTOCOL_BINARY_CMD_PREPENDQ,
        PROTOCOL_BINARY_CMD_REPLACE,
        PROTOCOL_BINARY_CMD_REPLACEQ,
        PROTOCOL_BINARY_CMD_SET,
        PROTOCOL_BINARY_CMD_SETQ,
        PROTOCOL_BINARY_CMD_TOUCH,
        PROTOCOL_BINARY_CMD_INVALID };

    uint8_t *ids;

    switch (type) {
    case CMD_TOTAL_MUTATION:
        ids = mutations;
        break;
    case CMD_TOTAL_RETRIVAL:
        ids = retrival;
        break;
    case CMD_TOTAL:
        ids = total;
        break;

    default:
        abort();
    }

    while (*ids != PROTOCOL_BINARY_CMD_INVALID) {
        ret += timings[*ids].total.load();
        ++ids;
    }

    return ret;
}
