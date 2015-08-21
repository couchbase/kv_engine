#pragma once

#include <stdint.h>

#ifdef WIN32
#include <winsock2.h>
#else

#include <arpa/inet.h>

#endif

#cmakedefine HAVE_HTONLL
#cmakedefine WORDS_BIGENDIAN

#ifdef __cplusplus
extern "C" {
#endif

#ifndef HAVE_HTONLL

#ifdef WORDS_BIGENDIAN
#define ntohll(value) value
#define htonll(value) value
#else
#define ntohll(value) byteswap64(value)
#define htonll(value) byteswap64(value)

extern uint64_t byteswap64(uint64_t value);

#endif

#endif /* HAVE_HTONLL */


#ifdef __cplusplus
}
#endif
