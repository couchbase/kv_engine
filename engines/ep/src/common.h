/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef SRC_COMMON_H
#define SRC_COMMON_H 1

#include "config.h"

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <memcached/engine.h>
#include <platform/platform.h>

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <list>
#include <sstream>
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
    char *endptr = NULL;
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
            if (strchr(str, '-') != NULL) {
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
            if (strchr(str, '-') != NULL) {
                return false;
            }
        }
        *out = ull;
        return true;
    }
    return false;
}

/**
 * Convert a time (in ns) to a human readable form...
 * @param time the time in nanoseconds
 * @return a string representation of the timestamp
 */
inline std::string hrtime2text(hrtime_t time) {
   const char * const extensions[] = { " ns", " usec", " ms", " s", NULL };
   int id = 0;

   while (time > 9999) {
      ++id;
      time /= 1000;
      if (extensions[id + 1] == NULL) {
         break;
      }
   }

   std::stringstream ss;
   if (extensions[id + 1] == NULL && time > 599) {
       int hour = static_cast<int>(time / 3600);
       time -= hour * 3600;
       int min = static_cast<int>(time / 60);
       time -= min * 60;
       ss << hour << "h:" << min << "m:" << time << "s";
   } else {
       ss << time << extensions[id];
   }

   return ss.str();
}

#endif  // SRC_COMMON_H_
