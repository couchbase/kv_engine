# ep-engine
# Copyright (C) 2010 Trond Norbye
# All rights reserved.
#
# Use and distribution licensed under the BSD license.  See
# the COPYING file in this directory for full text.
AC_PREREQ(2.59)
m4_include([m4/version.m4])
AC_INIT(ep-engine, VERSION_NUMBER, trond.norbye@gmail.com)
AC_CONFIG_SRCDIR([ep.hh])
AC_CONFIG_AUX_DIR(config)
AM_CONFIG_HEADER([config.h])
AC_CONFIG_MACRO_DIR([m4])
AC_DISABLE_STATIC

PANDORA_CANONICAL_TARGET(less-warnings, warnings-always-on, require-cxx,
                         force-gcc42)
PANDORA_SHARED_PTR
PANDORA_REQUIRE_PTHREAD
PANDORA_REQUIRE_LIBEVENT
PANDORA_HAVE_LIBCOUCHSTORE
PANDORA_ENABLE_DTRACE

AC_ARG_ENABLE([valgrind],
    [AS_HELP_STRING([--enable-valgrind],
            [Build with extra memsets to mask out false hits from valgrind. @<:@default=off@:>@])],
    [ac_enable_valgrind="$enableval"],
    [ac_enable_valgrind="no"])

AS_IF([test "$ac_enable_valgrind" = "yes"],
      [ AC_DEFINE(VALGRIND, 1, [Enable extra memset to help valgrind])])

AC_ARG_ENABLE([system-libsqlite3],
    [AS_HELP_STRING([--enable-system-libsqlite3],
            [Build with system installed libsqlite3. @<:@default=off@:>@])],
    [ac_enable_system_libsqlite3="$enableval"],
    [ac_enable_system_libsqlite3="no"])

AS_IF([test "$ac_enable_system_libsqlite3" = "yes"],
      [ PANDORA_REQUIRE_LIBSQLITE3
        AC_DEFINE(USE_SYSTEM_LIBSQLITE3, 1, "Set if you want to use a system installed sqlite3")
      ])
AM_CONDITIONAL(BUILD_EMBEDDED_LIBSQLITE3, test "$ac_enable_system_libsqlite3" = "no")

AC_ARG_ENABLE([generated-tests],
    [AS_HELP_STRING([--enable-generated-tests],
            [Run generated test suite. @<:@default=off@:>@])],
    [ac_enable_generated_tests="$enableval"],
    [ac_enable_generated_tests="no"])

AM_CONDITIONAL(BUILD_GENERATED_TESTS, test "$ac_enable_generated_tests" = "yes")

AC_DEFUN([AC_C_HTONLL],
[
    AC_CACHE_CHECK([for htonll],[ac_cv_have_htonll],[
    AC_RUN_IFELSE([
       AC_LANG_PROGRAM([[
#include <sys/types.h>
#include <netinet/in.h>
#include <inttypes.h>
       ]],[[
          return htonll(0);
       ]])
    ], [
      ac_cv_have_htonll=yes
    ],[
      ac_cv_have_htonll=no
    ])])

    AS_IF([test "x$ac_cv_have_htonll" = "xyes"],[
      AC_DEFINE([HAVE_HTONLL], [1], [Have ntohll])])
])

AC_C_HTONLL

AC_CHECK_HEADERS([arpa/inet.h])
AC_CHECK_HEADERS([netdb.h])
AC_CHECK_HEADERS([mach/mach_time.h])
AC_CHECK_HEADERS([poll.h])
AC_CHECK_HEADERS([atomic.h])
AC_CHECK_HEADERS([sysexits.h])
AC_CHECK_HEADERS([unistd.h])

AC_CHECK_HEADERS_ONCE([sys/socket.h
                       netinet/in.h
                       netinet/tcp.h
                       ws2tcpip.h
                       winsock2.h])

AC_CHECK_FUNCS(gethrtime)
AC_SEARCH_LIBS(clock_gettime, rt)
AC_CHECK_FUNCS(clock_gettime)
AC_CHECK_FUNCS(mach_absolute_time)
AC_CHECK_FUNCS(gettimeofday)
AC_CHECK_FUNCS(getopt_long)
AM_CONDITIONAL(BUILD_GETHRTIME, test "$ac_cv_func_gethrtime" = "no")

AC_LANG_PUSH(C++)
AC_CACHE_CHECK([Intel __sync_XXX intrinsics work],
               [av_cv_sync_intrinsics_work], [
  AC_TRY_LINK([#include <inttypes.h>],[
       uint64_t value;
       uint64_t oldValue;
       uint64_t newValue;
       __sync_bool_compare_and_swap(&value, oldValue, newValue);
       __sync_fetch_and_add(&value, newValue);

       return 0;
    ],[
      av_cv_sync_intrinsics_work=yes
    ], [
      # Check if our compiler is GCC and our target is x86 before
      # i586 so that we warn user that -march=i586 is required.
      SAVED_CPPFLAGS="$CPPFLAGS"
      CPPFLAGS="-march=i586 $SAVED_CPPFLAGS"
      AC_TRY_LINK([#include <inttypes.h>],[
        uint64_t value;
        uint64_t oldValue;
        uint64_t newValue;
        __sync_bool_compare_and_swap(&value, oldValue, newValue);
        __sync_fetch_and_add(&value, newValue);

        return 0;
      ], [
        av_cv_sync_intrinsics_work="-march=i586"
      ], [
        av_cv_sync_intrinsics_work=no
      ])
      CPPFLAGS="$SAVED_CPPFLAGS"
  ])
])
AC_LANG_POP()
AS_IF([test "x$av_cv_sync_intrinsics_work" = "x-march=i586"], [
  AC_MSG_WARN([Added -march=i586 to CPPFLAGS to have fast inline atomics. Consider building for -march=i686 or newer])
  AM_CPPFLAGS="$AM_CPPFLAGS -march=i586"
])
AS_IF([test "x$av_cv_sync_intrinsics_work" != "xno"], [
  AC_DEFINE([HAVE_GCC_ATOMICS], [1], [Have gcc atomics])
], [
  AS_IF([test "x$ac_cv_header_atomic_h" != "xyes"], [
    AC_MSG_WARN([Don't have Intel atomic intrinsics and Solaris atomic.h. Build will likely fail.])
  ])
])

AC_SEARCH_LIBS(dlopen, dl)

AC_ARG_WITH([memcached],
    [AS_HELP_STRING([--with-memcached],
      [Memcached source location])],
    [ac_cv_with_memcached="$withval"],
    [ac_cv_with_memcached=""])

AS_IF(test "x${ac_cv_with_memcached}" != "x",
      [CPPFLAGS="-I${ac_cv_with_memcached}/include $CPPFLAGS"
       PATH="${ac_cv_with_memcached}:$PATH"])

AC_CHECK_HEADERS(memcached/engine.h)
AS_IF(test "x${ac_cv_header_memcached_engine_h}" != "xyes",
      AC_MSG_ERROR([Can't build ep-engine without memcached/engine.h]))

AS_IF(test "x${bindir}" = 'x${exec_prefix}/bin',
      AS_IF(test "x${exec_prefix}" = "xNONE",
            PATH=${prefix}/bin:$PATH,
            PATH=${exec_prefix}/bin:$PATH),
      PATH=${bindir}:$PATH)

AC_PATH_PROG(ENGINE_TESTAPP, engine_testapp, no)
AS_IF(test "x${ENGINE_TESTAPP}" = "xno",
      AC_MSG_ERROR([Can't build ep-engine without engine_testapp]))

AM_CONDITIONAL(BUILD_BYTEORDER, test "x$ac_cv_have_htonll" != "xyes")

AS_IF([test "x$SUNCC" = "xyes"],
      [
        CFLAGS="-D_XOPEN_SOURCE=600 $CFLAGS"
        CPPFLAGS="-xldscope=hidden -mt $CPPFLAGS"
        CXXFLAGS="-erroff=truncwarn1,signextwarn,notused,inllargeuse,wunreachable $CXXFLAGS"
      ])

AC_SEARCH_LIBS(socket, socket)
AC_SEARCH_LIBS(gethostbyname, nsl)

AH_TOP([
#ifndef CONFIG_H
#define CONFIG_H
/*
 * Copyright (C) 2010 NorthScale, Inc
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in this directory for full text.
 */
#ifdef _SYS_FEATURE_TESTS_H
#error "You should include config.h as your first include file"
#endif
])

AH_BOTTOM([
#include "config_static.h"
#endif
])

AC_ARG_WITH([docs],
  [AS_HELP_STRING([--with-docs],
    [Generate documentation (yes|no) @<:@default=yes@:>@])],
  [with_docs=$withval],
  [with_docs=yes])

AS_IF([test "$with_docs" = "yes"],
  [
    REQUIRE_POD2MAN
  ])
AM_CONDITIONAL(BUILD_DOCS, test "$with_docs" = "yes")

AC_CACHE_CHECK([for gtest], [ac_cv_have_gtest], [
  AC_LANG_PUSH([C++])
  SAVED_LIBS="$LIBS"
  LIBS="-lgtest"
  AC_LINK_IFELSE(
    [AC_LANG_PROGRAM(
      [
#include "gtest/gtest.h"
      ],
      [
return 0;
      ])],
    [ac_cv_have_gtest=yes],
    [ac_cv_have_gtest=no])
  AC_LANG_POP([C++])
  LIBS="$SAVED_LIBS"
])

AM_CONDITIONAL(HAVE_GOOGLETEST, [test "$ac_cv_have_gtest" = "yes"])

dnl ----------------------------------------------------------------------------

AC_CONFIG_FILES(Makefile wrapper/wrapper)
AC_OUTPUT
echo "---"
echo "Configuration summary for $PACKAGE_NAME version $VERSION"
echo ""
echo "   * Installation prefix:       $prefix"
echo "   * System type:               $host_vendor-$host_os"
echo "   * Host CPU:                  $host_cpu"
echo "   * C Compiler:                $CC_VERSION"
echo "   * C++ Compiler:              $CXX_VERSION"
echo "   * Assertions enabled:        $ac_cv_assert"
echo "   * Debug enabled:             $with_debug"
echo "   * Warnings as failure:       $ac_cv_warnings_as_errors"
echo "   * Google test framework:     $ac_cv_have_gtest"
echo ""
AS_IF(test "$ac_cv_have_gtest" = "", [
      echo "**************************************************************"
      echo "You should consider installing gtest to be able to run tests.."
      echo "Mac users may run: brew install gtest"
      echo "Others may look at: http://code.google.com/p/googletest/"
      echo "**************************************************************"
      echo ""
      ])
echo "---"
