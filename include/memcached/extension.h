/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

typedef enum {
    EXTENSION_LOG_DEBUG = 1,
    EXTENSION_LOG_INFO,
    EXTENSION_LOG_NOTICE,
    EXTENSION_LOG_WARNING,
    EXTENSION_LOG_FATAL
} EXTENSION_LOG_LEVEL;

/**
 * Log extensions should provide the following rescriptor when
 * they register themselves. Please note that if you register a log
 * extension it will <u>replace</u> old one. If you want to be nice to
 * the user you should allow your logger to be chained.
 *
 * Please note that the memcached server will <b>not</b> call the log
 * function if the verbosity level is too low. This is a perfomance
 * optimization from the core to avoid potential formatting of output
 * that may be thrown away.
 */
typedef struct {
    /**
     * Add an entry to the log.
     * @param severity the severity for this log entry
     * @param client_cookie the client we're serving (may be NULL if not
     *                      known)
     * @param fmt format string to add to the log
     */
    void (*log)(EXTENSION_LOG_LEVEL severity,
                const void* client_cookie,
                const char* fmt,
                ...);
} EXTENSION_LOGGER_DESCRIPTOR;

typedef struct {
    EXTENSION_LOGGER_DESCRIPTOR* (*get_logger)(void);
    EXTENSION_LOG_LEVEL (*get_level)(void);
    void (*set_level)(EXTENSION_LOG_LEVEL severity);
} SERVER_LOG_API;
