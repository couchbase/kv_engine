#ifndef COMMON_H
#define COMMON_H 1

#include <cassert>
#include <errno.h>
#include <stdlib.h>
#include <stdint.h>
#include <ctype.h>
#include <inttypes.h>
#include <limits.h>

#include <math.h>
#include <sys/time.h>
#include <memcached/engine.h>

#include <vector>
#include <list>

#include "config.h"

#if defined(HAVE_MEMORY)
# include <memory>
#endif
#if defined(HAVE_TR1_MEMORY)
# include <tr1/memory>
#endif
#if defined(HAVE_BOOST_SHARED_PTR_HPP)
# include <boost/shared_ptr.hpp>
#endif

#if defined(SHARED_PTR_NAMESPACE)
using SHARED_PTR_NAMESPACE::shared_ptr;
#else
# error No shared pointer implementation found!
#endif

#include <sstream>

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
extern EXTENSION_LOGGER_DESCRIPTOR *getLogger(void);

extern "C" {
    extern rel_time_t (*ep_current_time)();
    extern time_t (*ep_abs_time)(rel_time_t);
    extern rel_time_t (*ep_reltime)(time_t);
    extern time_t ep_real_time();
}

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

/**
 * Convert a time (in ns) to a human readable form...
 * @param time the time in nanoseconds
 * @return a string representation of the timestamp
 */
inline std::string hrtime2text(hrtime_t time) {
   const char * const extensions[] = { " usec", " ms", " s", NULL };
   int id = 0;

   while (time > 9999) {
      ++id;
      time /= 1000;
      if (extensions[id + 1] == NULL) {
         break;
      }
   }

   std::stringstream ss;
   ss << time << extensions[id];
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

#define GIGANTOR ((size_t)1<<(sizeof(size_t)*8-1))
#endif /* COMMON_H */
