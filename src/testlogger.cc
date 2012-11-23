/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "config.h"

#include "common.h"

extern "C" {
static const char* test_get_logger_name(void) {
    return "testlogger";
}

static void test_get_logger_log(EXTENSION_LOG_LEVEL,
                                const void* , // cookie
                                const char *, ...) {
    // ignore
}
}

EXTENSION_LOGGER_DESCRIPTOR* getLogger(void) {
    static EXTENSION_LOGGER_DESCRIPTOR logger;
    logger.get_name = test_get_logger_name;
    logger.log = test_get_logger_log;
    return &logger;
}
