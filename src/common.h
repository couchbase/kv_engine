/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#ifndef SRC_COMMON_H_
#define SRC_COMMON_H_ 1

#include "config.h"

#if defined(HAVE_BOOST_SHARED_PTR_HPP)
# include <boost/shared_ptr.hpp>
#endif
#if defined(HAVE_BOOST_UNORDERED_MAP_HPP)
# include <boost/unordered_map.hpp>
#endif

#include <ctype.h>
#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <math.h>
#include <memcached/engine.h>
#include <platform/platform.h>

#if defined(WIN32)
#include <pthread.h>

static inline int my_pthread_cond_timedwait(pthread_cond_t *restrict cond,
                                            pthread_mutex_t *restrict mutex,
                                            const struct timespec *restrict abstime) {
    int rv = pthread_cond_timedwait(cond, mutex, abstime);
    //On 64bit Cygwin/mingw, it is redefined ETIMEDOUT to WSAETIMEDOUT,
    //which is 10060. Here we still keep the original ETIMEDOUT value
    // for return value of functions such as pthread_cond_timedwait
    // NOTE: wsasock2.h is included in win32/config.h so we expect this define to be made
    // But on 32bit Cygwin/mingw, we just keep the original return value.
    if (rv == 110) {
        rv = ETIMEDOUT;
    }
    return rv;
}

#define pthread_cond_timedwait(a,b,c) my_pthread_cond_timedwait(a,b,c)

#endif


#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#if defined(HAVE_TR1_MEMORY)
# include <tr1/memory>
#endif

#if defined(HAVE_TR1_UNORDERED_MAP)
# include <tr1/unordered_map>
#endif

#include <cassert>
#include <list>

#if defined(HAVE_MEMORY)
# include <memory>
#endif

#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "ep_time.h"

#if defined(SHARED_PTR_NAMESPACE)
using SHARED_PTR_NAMESPACE::shared_ptr;
#else
# error No shared pointer implementation found!
#endif

#if defined(UNORDERED_MAP_NAMESPACE)
using UNORDERED_MAP_NAMESPACE::unordered_map;
#else
# error No unordered_map implementation found!
#endif

/* Linux' limits don't bring this in in c++ mode without doing weird
   stuff.  It's a known constant, so we'll just make it if we don't
   have it. */
#ifndef UINT16_MAX
#define UINT16_MAX 65535
#endif /* UINT16_MAX */

// Stolen from http://google-styleguide.googlecode.com/svn/trunk/cppguide.xml
// A macro to disallow the copy constructor and operator= functions
// This should be used in the private: declarations for a class
#define DISALLOW_COPY_AND_ASSIGN(TypeName)      \
    TypeName(const TypeName&);                  \
    void operator=(const TypeName&)

// Utility functions implemented in various modules.

extern void LOG(EXTENSION_LOG_LEVEL severity, const char *fmt, ...);

extern ALLOCATOR_HOOKS_API *getHooksApi(void);

// Time handling functions
inline void advance_tv(struct timeval &tv, const double secs) {
    double ip, fp;
    fp = modf(secs, &ip);
    int usec = static_cast<int>(fp * 1e6) + static_cast<int>(tv.tv_usec);
    int quot = usec / 1000000;
    int rem = usec % 1000000;
    tv.tv_sec = static_cast<int>(ip) + tv.tv_sec + quot;
    tv.tv_usec = rem;
}

inline bool less_tv(const struct timeval &tv1, const struct timeval &tv2) {
    if (tv1.tv_sec == tv2.tv_sec) {
        return tv1.tv_usec < tv2.tv_usec;
    } else {
        return tv1.tv_sec < tv2.tv_sec;
    }
}

inline void set_max_tv(struct timeval &tv) {
    tv.tv_sec = INT_MAX;  // until the year 2038!
    tv.tv_usec = INT_MAX;
}

inline bool is_max_tv(struct timeval &tv) {
    if (tv.tv_sec == INT_MAX && tv.tv_usec == INT_MAX) {
        return true;
    }
    return false;
}

inline bool parseUint16(const char *in, uint16_t *out) {
    assert(out != NULL);
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
    assert(out);
    assert(str);
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

#define xisspace(c) isspace((unsigned char)c)
inline bool parseUint64(const char *str, uint64_t *out) {
    assert(out != NULL);
    errno = 0;
    *out = 0;
    char *endptr;
    unsigned long long ull = strtoull(str, &endptr, 10);
    if (errno == ERANGE)
        return false;
    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((long long) ull < 0) {
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

/**
 * Given a vector instance with the sorted elements and a chunk size, this will creates
 * the list of chunks where each chunk represents a specific range and contains the chunk
 * size of elements within that range.
 * @param elm_list the vector instance that has the list of sorted elements to get chunked
 * @param chunk_size the size of each chunk
 * @param chunk_list the list of chunks to be returned to the caller
 */
template <typename T>
void createChunkListFromArray(std::vector<T> *elm_list, size_t chunk_size,
                     std::list<std::pair<T, T> > &chunk_list) {
    size_t counter = 0;
    std::pair<T, T> chunk_range;

    if (elm_list->empty() || chunk_size == 0) {
        return;
    }

    typename std::vector<T>::iterator iter;
    typename std::vector<T>::iterator iterend(elm_list->end());
    --iterend;
    for (iter = elm_list->begin(); iter != elm_list->end(); ++iter) {
        ++counter;
        if (counter == 1) {
            chunk_range.first = *iter;
        }

        if (counter == chunk_size || iter == iterend) {
            chunk_range.second = *iter;
            chunk_list.push_back(chunk_range);
            counter = 0;
        }
    }
}

/**
 * Return true if the elements in a given container are sorted in the ascending order.
 * @param first Forward iterator to the initial position of the container.
 * @param last Forward iterator to the final position of the container. The element
 * pointed by this iterator is not included.
 * @param compare Comparison function that returns true if the first element is less than
 * equal to the second element.
 */
template <class ForwardIterator, class Compare>
bool sorted(ForwardIterator first, ForwardIterator last, Compare compare) {
    bool is_sorted = true;
    ForwardIterator next;
    for (; first != last; ++first) {
        next = first;
        if (++next != last && !compare(*first, *next)) {
            is_sorted = false;
            break;
        }
    }
    return is_sorted;
}

#define GIGANTOR ((size_t)1<<(sizeof(size_t)*8-1))
#endif  // SRC_COMMON_H_
