# ep-engine
# Copyright (C) 2010 Trond Norbye
# All rights reserved.
#
# Use and distribution licensed under the BSD license.  See
# the COPYING file in this directory for full text.
AC_PREREQ(2.61)
m4_include([m4/version.m4])
AC_INIT(epengine, VERSION_NUMBER, trond.norbye@gmail.com)
AC_CONFIG_SRCDIR([suite.hh])
AC_CONFIG_AUX_DIR(config)
AM_CONFIG_HEADER([config.h])
AC_CONFIG_MACRO_DIR([m4])

PANDORA_CANONICAL_TARGET

PANDORA_HAVE_LIBSQLITE3

AS_IF([test "x$SUNCC" = "xyes"],
      [
        CPPFLAGS="-xldscope=hidden $CPPFLAGS"
        CXXFLAGS="-erroff=truncwarn1,signextwarn,notused,inllargeuse $CXXFLAGS"
      ],
      [ AS_IF([test "x$GCC" = "xyes" ],
              [ CPPFLAGS="-fvisibility=hidden $CPPFLAGS" ])
     ])

AH_BOTTOM([
#if ((defined (__SUNPRO_C) || defined(__SUNPRO_CC)) || defined GCC)
#define EXPORT_FUNCTION __attribute__ ((visibility("default")))
#else
#define EXPORT_FUNCTION
#endif
])

AC_CONFIG_FILES(Makefile)
AC_OUTPUT
echo "---"
echo "Configuration summary for $PACKAGE_NAME version $VERSION"
echo ""
echo "   * Installation prefix:       $prefix"
echo "   * System type:               $host_vendor-$host_os"
echo "   * Host CPU:                  $host_cpu"
echo "   * C Compiler:                $CC_VERSION"
echo "   * Assertions enabled:        $ac_cv_assert"
echo "   * Debug enabled:             $with_debug"
echo "   * Warnings as failure:       $ac_cv_warnings_as_errors"
echo ""
echo "---"
