#
#     Copyright 2010, 2011, 2012 Couchbase, Inc.
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#
AC_PREREQ(2.59)
m4_include([m4/version.m4])
AC_INIT(ep-engine, VERSION_NUMBER, support@couchbase.com)
AC_CONFIG_SRCDIR([README.markdown])
AC_CONFIG_AUX_DIR(config)

# Preserve what the user specified as CFLAGS/CXXFLAGS/CPPFLAGS/LDFLAGS
SAVE_CFLAGS="$CFLAGS"
SAVE_CXXFLAGS="$CXXFLAGS"
SAVE_CPPFLAGS="$CPPFLAGS"
SAVE_LDFLAGS="$LDFLAGS"

# The stupid automake system thinks it is a good idea to inject
# -g -O2 to C(XX)FLAGS if it isn't set..
# Why I don't like this? Well -g is in conflict with -g3 that I'm
# adding for Sun Studio
AS_IF([test "x$CFLAGS" = "x" ],
      [CFLAGS="-I."])
AS_IF([test "x$CXXFLAGS" = "x" ],
      [CXXFLAGS="-I."])
AS_IF([test "x$CPPFLAGS" = "x" ],
      [CPPFLAGS="-I."])

AC_USE_SYSTEM_EXTENSIONS
AM_INIT_AUTOMAKE(subdir-objects foreign)
AC_PROG_CXX
AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_LN_S
AC_DISABLE_STATIC
AC_PROG_LIBTOOL
AC_CONFIG_HEADERS([src/config.h])
AC_CONFIG_MACRO_DIR([m4])

# Restore CFLAGS/CXXFLAGS now that I've prevented automake
# to inject the extra options ;)
AS_IF([test "x$CFLAGS" = "x-I." ],
      [CFLAGS=""])
AS_IF([test "x$CXXFLAGS" = "x-I." ],
      [CXXFLAGS=""])
AS_IF([test "x$CPPFLAGS" = "x-I." ],
      [CPPFLAGS=""])

# the malloc tests seems to be broken for cross compilation.. ignore them
ac_cv_func_malloc_0_nonnull=yes
ac_cv_func_realloc_0_nonnull=yes

COUCHBASE_GENERIC_COMPILER(c99 c++ pthread)

AS_IF([test "$ac_cv_enable_gcov" = "yes"],
      [ LIBS="$LIBS -lgcov" ])

AC_SEARCH_LIBS(pthread_create, pthread)
if test "x$ac_cv_search_pthread_create" = "xno"; then
  AC_MSG_ERROR([Can't enable threads without the POSIX thread library.])
fi

AC_CHECK_HEADERS_ONCE([arpa/inet.h netdb.h mach/mach_time.h poll.h
                       atomic.h sysexits.h unistd.h sys/socket.h
                       netinet/in.h netinet/tcp.h ws2tcpip.h
                       winsock2.h])

AC_LANG_PUSH(C++)
AC_CHECK_HEADERS([memory tr1/memory boost/shared_ptr.hpp])
AC_CACHE_CHECK([the namespace of shared_ptr],
  [ac_cv_shared_namespace],[
     for namespace in std tr1 std::tr1 boost
     do
       AC_COMPILE_IFELSE(
          [AC_LANG_PROGRAM([[
#if defined(HAVE_MEMORY)
# include <memory>
#endif
#if defined(HAVE_TR1_MEMORY)
# include <tr1/memory>
#endif
#if defined(HAVE_BOOST_SHARED_PTR_HPP)
# include <boost/shared_ptr.hpp>
#endif
#include <string>

using $namespace::shared_ptr;
using namespace std;
            ]],[[
shared_ptr<string> test_ptr(new string("test string"));
            ]])],
            [
              ac_cv_shared_ptr_namespace="${namespace}"
              break
            ],[ac_cv_shared_ptr_namespace=missing])
       done
  ])
AS_IF([test "x$ac_cv_shared_ptr_namespace" = "missing" ],
      AC_MSG_ERROR([Can't build ep-engine without a known shared_ptr]))

AC_DEFINE_UNQUOTED([SHARED_PTR_NAMESPACE],
                   ${ac_cv_shared_ptr_namespace},
                   [The namespace in which SHARED_PTR can be found])

AC_CHECK_HEADERS([tr1/unordered_map boost/unordered_map.hpp])
AC_CACHE_CHECK([the location of unordered_map header file],
    [ac_cv_unordered_namespace],[
      for namespace in std tr1 std::tr1 boost
      do
        AC_COMPILE_IFELSE(
          [AC_LANG_PROGRAM([[
#if defined(HAVE_TR1_UNORDERED_MAP)
# include <tr1/unordered_map>
#endif
#if defined(HAVE_BOOST_UNORDERED_MAP_HPP)
# include <boost/unordered_map.hpp>
#endif
#include <string>

using $namespace::unordered_map;
using namespace std;
            ]],[[
unordered_map<string, string> test_map;
            ]])],
            [
              ac_cv_unordered_map_namespace="${namespace}"
              break
            ],[ac_cv_unordered_map_namespace=missing])
       done
  ])

AC_DEFINE_UNQUOTED([UNORDERED_MAP_NAMESPACE],
                   ${ac_cv_unordered_map_namespace},
                   [The namespace in which UNORDERED_MAP can be found])

AS_IF([test "x$ac_cv_unordered_map_namespace" = "missing" ],
      AC_MSG_ERROR([Can't build ep-engine without a known unordered_map]))

AC_LANG_POP()


saved_LIBS="$LIBS"
LIBS="$LIBS -lcouchstore"
AC_CACHE_CHECK([for libcouchstore],[ac_cv_have_libcouchstore],[
   AC_TRY_LINK([
      #include <sys/types.h>
      #include <stdlib.h>
      #include <libcouchstore/couch_db.h>
       ],[
          couchstore_close_db(NULL);
       ], [
          ac_cv_have_libcouchstore=yes
       ], [
          ac_cv_have_libcouchstore=no
       ])
   ])
LIBS="$saved_LIBS"

AS_IF([test "x$ac_cv_have_libcouchstore" = "xyes"],
      [ AC_DEFINE([HAVE_LIBCOUCHSTORE], [1], [Have libcouchstore])
        LTLIBCOUCHSTORE=-lcouchstore
      ], [
        AC_MSG_ERROR([Can't build ep-engine without libcouchstore])
      ])

AC_SUBST(LTLIBCOUCHSTORE)
AM_CONDITIONAL(HAVE_LIBCOUCHSTORE, [test "x$ac_cv_have_libcouchstore" = "xyes"])

AC_CACHE_CHECK([for libevent >= 1.4], [ac_cv_have_libevent],
  [ saved_libs="$LIBS"
    LIBS="$LIBS -levent"
    AC_TRY_LINK([
      #include <sys/types.h>
      #include <event.h>
            ],[
      event_base_new();
            ],[
      ac_cv_have_libevent="yes"
            ], [
      ac_cv_have_libevent="no"
      ])
    LIBS="$saved_libs"
  ])
LIBS="$saved_LIBS"
AS_IF([test "x$ac_cv_have_libevent" = "xyes"],
      [ AC_DEFINE([HAVE_LIBEVENT], [1], [Have libevent])
        LTLIBEVENT=-levent
      ])
AC_SUBST(LTLIBEVENT)

AC_ARG_ENABLE([valgrind],
    [AS_HELP_STRING([--enable-valgrind],
            [Build with extra memsets to mask out false hits from valgrind. @<:@default=off@:>@])],
    [ac_enable_valgrind="$enableval"],
    [ac_enable_valgrind="no"])

AS_IF([test "$ac_enable_valgrind" = "yes"],
      [ AC_DEFINE(VALGRIND, 1, [Enable extra memset to help valgrind])])

AC_ARG_ENABLE([generated-tests],
    [AS_HELP_STRING([--enable-generated-tests],
            [Run generated test suite. @<:@default=off@:>@])],
    [ac_enable_generated_tests="$enableval"],
    [ac_enable_generated_tests="no"])

AM_CONDITIONAL(BUILD_GENERATED_TESTS, test "$ac_enable_generated_tests" = "yes")

AC_SEARCH_LIBS(clock_gettime, rt)
AC_CHECK_FUNCS(clock_gettime)
AC_CHECK_FUNCS(mach_absolute_time)
AC_CHECK_FUNCS(gettimeofday)
AC_CHECK_FUNCS(getopt_long)

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
       AM_CPPFLAGS="$AM_CPPFLAGS $CPPFLAGS"
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

AC_SEARCH_LIBS(socket, socket)
AC_SEARCH_LIBS(gethostbyname, nsl)

AH_TOP([
/*
 *     Copyright 2010 Couchbase, Inc.
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

#ifndef SRC_CONFIG_H_
#define SRC_CONFIG_H_ 1
#ifdef _SYS_FEATURE_TESTS_H
#error "You should include config.h as your first include file"
#endif
])

AH_BOTTOM([
#include "config_static.h"
#endif  // SRC_CONFIG_H_
])

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

dnl restore CFLAGS, CPPFLAGS, CXXFLAGS and LDFLAGS
CFLAGS="$SAVE_CFLAGS"
CXXFLAGS="$SAVE_CXXFLAGS"
CPPFLAGS="$SAVE_CPPFLAGS"
LDFLAGS="$SAVE_LDFLAGS"

dnl ----------------------------------------------------------------------------

AC_CONFIG_FILES(Makefile wrapper/wrapper)
AC_OUTPUT
AS_IF(test "$ac_cv_have_gtest" = "", [
      echo "**************************************************************"
      echo "You should consider installing gtest to be able to run tests.."
      echo "Mac users may run: brew install gtest"
      echo "Others may look at: http://code.google.com/p/googletest/"
      echo "**************************************************************"
      echo ""
      ])
echo "---"
