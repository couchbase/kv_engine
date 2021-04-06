/*
 * Portions Copyright (c) 2010-Present Couchbase
 * Portions Copyright (c) 2008-2010 Danga Interactive
 * Portions Copyright (c) 2009 Sun Microsystems
 *
 * Use of this software is governed by the Apache License, Version 2.0 and
 * BSD 3 Clause included in the files licenses/APL.txt and
 * licenses/BSD-3-Clause-Danga-Interactive.txt
 * licenses/BSD-3-Clause-Sun-Microsystems.txt
 */
#include <cctype>
#include <cerrno>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>

#include <memcached/engine.h>
#include <memcached/util.h>

/* Avoid warnings on solaris, where isspace() is an index into an array, and gcc uses signed chars */
#define xisspace(c) isspace((unsigned char)c)

bool safe_strtoull(const char* str, uint64_t& out) {
    char *endptr;
    uint64_t ull;

    errno = 0;
    out = 0;

    ull = strtoull(str, &endptr, 10);
    if (errno == ERANGE) {
        return false;
    }

    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((int64_t)ull < 0) {
            /* only check for negative signs in the uncommon case when
             * the unsigned number is so big that it's negative as a
             * signed number. */
            if (strchr(str, '-') != nullptr) {
                return false;
            }
        }
        out = ull;
        return true;
    }
    return false;
}

bool safe_strtoll(const char* str, int64_t& out) {
    char *endptr;
    int64_t ll;

    errno = 0;
    out = 0;
    ll = strtoll(str, &endptr, 10);

    if (errno == ERANGE) {
        return false;
    }
    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        out = ll;
        return true;
    }
    return false;
}

bool safe_strtoul(const char* str, uint32_t& out) {
    char *endptr = nullptr;
    unsigned long l = 0;
    out = 0;
    errno = 0;

    l = strtoul(str, &endptr, 10);
    if (errno == ERANGE) {
        return false;
    }

    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((long) l < 0) {
            /* only check for negative signs in the uncommon case when
             * the unsigned number is so big that it's negative as a
             * signed number. */
            if (strchr(str, '-') != nullptr) {
                return false;
            }
        }
        out = l;
        return true;
    }

    return false;
}

bool safe_strtol(const char* str, int32_t& out) {
    char *endptr;
    long l;
    errno = 0;
    out = 0;
    l = strtol(str, &endptr, 10);

    if (errno == ERANGE) {
        return false;
    }
    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        out = l;
        return true;
    }
    return false;
}

bool safe_strtof(const char* str, float& out) {
#ifdef WIN32
    /* Check for illegal charachters */
    const char *ptr = str;
    int space = 0;
    while (*ptr != '\0') {
        if (!isdigit(*ptr)) {
            switch (*ptr) {
            case '.':
            case ',':
            case '+':
            case '-':
                break;

            case ' ':
                ++space;
                break;
            default:
                return false;
            }
        }
        ++ptr;
        if (space) {
            break;
        }
    }


    if (ptr == str) {
        /* Empty string */
        return false;
    }
    out = (float)atof(str);
    if (errno == ERANGE) {
        return false;
    }
    return true;
#else
    char *endptr;
    float l;
    errno = 0;
    out = 0;
    l = strtof(str, &endptr);
    if (errno == ERANGE) {
        return false;
    }
    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        out = l;
        return true;
    }
    return false;
#endif
}

std::string to_string(const BucketCompressionMode mode) {
    switch (mode) {
    case BucketCompressionMode::Off:
        return "off";
    case BucketCompressionMode::Passive:
        return "passive";
    case BucketCompressionMode::Active:
        return "active";
    }

    throw std::invalid_argument(
            "to_string(BucketCompressionMode): Invalid mode: " +
            std::to_string(int(mode)));
}

BucketCompressionMode parseCompressionMode(const std::string& mode) {
    if (mode == "off") {
        return BucketCompressionMode::Off;
    } else if (mode == "passive") {
        return BucketCompressionMode::Passive;
    } else if (mode == "active") {
        return BucketCompressionMode::Active;
    } else {
        throw std::invalid_argument(
                "setCompressionMode: invalid mode specified");
    }
}
