#include "config.h"
#include <stdio.h>
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>

#include "memcached/util.h"

/* Avoid warnings on solaris, where isspace() is an index into an array, and gcc uses signed chars */
#define xisspace(c) isspace((unsigned char)c)

bool safe_strtoull(const char *str, uint64_t *out) {
    char *endptr;
    uint64_t ull;

    cb_assert(out != NULL);
    errno = 0;
    *out = 0;

    ull = strtoull(str, &endptr, 10);
    if (errno == ERANGE) {
        return false;
    }

    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((int64_t)ull < 0) {
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

bool safe_strtoll(const char *str, int64_t *out) {
    char *endptr;
    int64_t ll;

    cb_assert(out != NULL);
    errno = 0;
    *out = 0;
    ll = strtoll(str, &endptr, 10);

    if (errno == ERANGE) {
        return false;
    }
    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = ll;
        return true;
    }
    return false;
}

bool safe_strtoul(const char *str, uint32_t *out) {
    char *endptr = NULL;
    unsigned long l = 0;
    cb_assert(out);
    cb_assert(str);
    *out = 0;
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
            if (strchr(str, '-') != NULL) {
                return false;
            }
        }
        *out = l;
        return true;
    }

    return false;
}

bool safe_strtol(const char *str, int32_t *out) {
    char *endptr;
    long l;
    cb_assert(out != NULL);
    errno = 0;
    *out = 0;
    l = strtol(str, &endptr, 10);

    if (errno == ERANGE) {
        return false;
    }
    if (xisspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = l;
        return true;
    }
    return false;
}

bool safe_strtof(const char *str, float *out) {
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
    *out = (float)atof(str);
    if (errno == ERANGE) {
        return false;
    }
    return true;
#else
    char *endptr;
    float l;
    cb_assert(out != NULL);
    errno = 0;
    *out = 0;
    l = strtof(str, &endptr);
    if (errno == ERANGE) {
        return false;
    }
    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = l;
        return true;
    }
    return false;
#endif
}

void vperror(const char *fmt, ...) {
    int old_errno = errno;
    char buf[1024];
    va_list ap;

    va_start(ap, fmt);
    if (vsnprintf(buf, sizeof(buf), fmt, ap) == -1) {
        buf[sizeof(buf) - 1] = '\0';
    }
    va_end(ap);

    errno = old_errno;

    perror(buf);
}

const char *memcached_protocol_errcode_2_text(protocol_binary_response_status err) {
    switch (err) {
    case PROTOCOL_BINARY_RESPONSE_SUCCESS:
        return "Success";
    case PROTOCOL_BINARY_RESPONSE_KEY_ENOENT:
        return "Not found";
    case PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS:
        return "Data exists for key";
    case PROTOCOL_BINARY_RESPONSE_E2BIG:
        return "Too large";
    case PROTOCOL_BINARY_RESPONSE_EINVAL:
        return "Invalid arguments";
    case PROTOCOL_BINARY_RESPONSE_NOT_STORED:
        return "Not stored";
    case PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL:
        return "Non-numeric server-side value for incr or decr";
    case PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET:
        return "I'm not responsible for this vbucket";
    case PROTOCOL_BINARY_RESPONSE_NO_BUCKET:
        return "Not connected to a bucket";
    case PROTOCOL_BINARY_RESPONSE_AUTH_STALE:
        return "Authentication stale. Please reauthenticate";
    case PROTOCOL_BINARY_RESPONSE_AUTH_ERROR:
        return "Auth failure";
    case PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE:
        return "Auth continue";
    case PROTOCOL_BINARY_RESPONSE_ERANGE:
        return "Outside range";
    case PROTOCOL_BINARY_RESPONSE_ROLLBACK:
        return "Rollback";
    case PROTOCOL_BINARY_RESPONSE_EACCESS:
        return "No access";
    case PROTOCOL_BINARY_RESPONSE_NOT_INITIALIZED:
        return "Node not initialized";
    case PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND:
        return "Unknown command";
    case PROTOCOL_BINARY_RESPONSE_ENOMEM:
        return "Out of memory";
    case PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED:
        return "Not supported";
    case PROTOCOL_BINARY_RESPONSE_EINTERNAL:
        return "Internal error";
    case PROTOCOL_BINARY_RESPONSE_EBUSY:
        return "Server too busy";
    case PROTOCOL_BINARY_RESPONSE_ETMPFAIL:
        return "Temporary failure";

    /* Sub-document responses */
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT:
        return "Subdoc: Path not does not exist";
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH:
        return "Subdoc: Path mismatch";
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EINVAL:
        return "Subdoc: Invalid path";
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_E2BIG:
        return "Subdoc: Path too large";
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_E2DEEP:
        return "Subdoc: Document too deep";
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_VALUE_CANTINSERT:
        return "Subdoc: Cannot insert specified value";
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_NOTJSON:
        return "Subdoc: Existing document not JSON";
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_NUM_ERANGE:
        return "Subdoc: Existing number outside valid arithmetic range";
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_DELTA_ERANGE:
         return "Subdoc: Delta outside valid arithmetic range";
    case PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS:
         return "Subdoc: Document path already exists";

    default:
        return "Unknown error code";
    }
}
