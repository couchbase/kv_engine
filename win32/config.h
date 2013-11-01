#ifndef CONFIG_H
#define CONFIG_H

#include "config_version.h"

#undef  _WIN32_WINNT
#define _WIN32_WINNT    0x0501        /* Needed to resolve getaddrinfo et al. */

#include <winsock2.h>
#include <ws2tcpip.h>

// there seems to be a bad macro redefine for sleep in unistd.h, so let's
// include it before any other files..
#include <unistd.h>
#include "win32.h"

/* Windows doesn't have sleep, but Sleep(millisecond) */

#define sleep(a) Sleep(a * 1000)
// This doesn't work for our short sleeps... figure out a better one
#define usleep(a) sleep(a / 1000)

#define HAVE_ASSERT_H 1
#define HAVE_DLFCN_H 1
#define HAVE_INTTYPES_H 1
#define HAVE_MALLOC 1
#define HAVE_MEMORY 1
#define HAVE_MEMORY_H 1
#define HAVE_REALLOC 1
#define HAVE_STDCXX_0X
#define HAVE_STDINT_H 1
#define HAVE_STDLIB_H 1
#define HAVE_STRINGS_H 1
#define HAVE_STRING_H 1
#define HAVE_SYS_STAT_H 1
#define HAVE_SYS_TYPES_H 1
#define HAVE_TR1_MEMORY 1
#define HAVE_TR1_UNORDERED_MAP 1
#define HAVE_UNISTD_H 1
#define HOST_CPU "i686"
#define HOST_OS "mingw32"
#define HOST_VENDOR "pc"
#define LT_OBJDIR ".libs/"
#define SHARED_PTR_NAMESPACE std
#define UNORDERED_MAP_NAMESPACE std::tr1
#define STDCXX_98_HEADERS
#define STDC_HEADERS 1
#define TARGET_CPU "i686"
#define TARGET_OS "mingw32"
#define TARGET_VENDOR "pc"
#define TIME_WITH_SYS_TIME 1
#ifndef _GNU_SOURCE
# define _GNU_SOURCE 1
#endif
#define restrict __restrict
#if ((defined (__SUNPRO_C) || defined(__SUNPRO_CC)) || defined __GNUC__)
#define EXPORT_FUNCTION __attribute__ ((visibility("default")))
#else
#define EXPORT_FUNCTION
#endif
#define HAVE_GETTIMEOFDAY 0
#define HAVE_QUERYPERFORMANCECOUNTER 1

#define EAI_SYSTEM -11

#define HAVE_GCC_ATOMICS 1

#define AUTOCONF_BUILD 1

#include "config_static.h"

#if defined(__cplusplus) && defined(__WIN64__)
// There is a bug in the headerfiles from the 64bit mingw compiler I'm
// using that _Exit isn't defined... Let's just add the C99 definition
// here...
extern "C" void _Exit(int status);
#endif

#undef small
#undef interface

#endif
