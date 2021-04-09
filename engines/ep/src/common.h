/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <memcached/engine.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <iosfwd>
#include <list>
#include <utility>
#include <vector>

#include "ep_time.h"


/* Linux' limits don't bring this in in c++ mode without doing weird
   stuff.  It's a known constant, so we'll just make it if we don't
   have it. */
#ifndef UINT16_MAX
#define UINT16_MAX 65535
#endif /* UINT16_MAX */

inline bool parseUint16(const char *in, uint16_t *out) {
    if (out == nullptr) {
        return false;
    }

    errno = 0;
    *out = 0;
    char *endptr;
    long num = strtol(in, &endptr, 10);
    if (errno == ERANGE || num < 0 || num > (long)UINT16_MAX) {
        return false;
    }
    if (isspace(*endptr) || (*endptr == '\0' && endptr != in)) {
        *out = static_cast<uint16_t>(num);
        return true;
    }
    return false;
}

inline bool parseUint32(const char *str, uint32_t *out) {
    char *endptr = nullptr;
    if (out == nullptr || str == nullptr) {
        return false;
    }
    *out = 0;
    errno = 0;

    unsigned long l = strtoul(str, &endptr, 10);
    if (errno == ERANGE) {
        return false;
    }

    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((long) l < 0) {
            /* only check for negative signs in the uncommon case when
             * the unsigned number is so big that it's negative as a
             * signed number. */
            if (strchr(str, '-') != nullptr) {
                return false;
            }
        }
        *out = l;
        return true;
    }

    return false;
}

inline bool parseInt64(const char *str, int64_t *out) {
    if (out == nullptr) {
        return false;
    }
    errno = 0;
    *out = 0;
    char *endptr;

    int64_t ll = strtoll(str, &endptr, 10);
    if (errno == ERANGE) {
        return false;
    }

    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = static_cast<int64_t>(ll);
        return true;
    }

    return false;
}

inline bool parseInt64(const std::string& str, int64_t* out) {
    return parseInt64(str.c_str(), out);
}

#define xisspace(c) isspace((unsigned char)c)
inline bool parseUint64(const char *str, uint64_t *out) {
    if (out == nullptr) {
        return false;
    }
    errno = 0;
    *out = 0;
    char *endptr;
    uint64_t ull = strtoull(str, &endptr, 10);
    if (errno == ERANGE)
        return false;
    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((int64_t) ull < 0) {
            /* only check for negative signs in the uncommon case when
             * the unsigned number is so big that it's negative as a
             * signed number. */
            if (strchr(str, '-') != nullptr) {
                return false;
            }
        }
        *out = ull;
        return true;
    }
    return false;
}

inline bool parseUint64(const std::string& str, uint64_t* out) {
    return parseUint64(str.c_str(), out);
}
