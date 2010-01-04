dnl  Copyright (C) 2009 Sun Microsystems
dnl This file is free software; Sun Microsystems
dnl gives unlimited permission to copy and/or distribute it,
dnl with or without modifications, as long as this notice is preserved.

AC_DEFUN([_PANDORA_SEARCH_LIBMEMCACHED],[
  AC_REQUIRE([AC_LIB_PREFIX])

  dnl --------------------------------------------------------------------
  dnl  Check for libmemcached
  dnl --------------------------------------------------------------------

  AC_ARG_ENABLE([libmemcached],
    [AS_HELP_STRING([--disable-libmemcached],
      [Build with libmemcached support @<:@default=on@:>@])],
    [ac_enable_libmemcached="$enableval"],
    [ac_enable_libmemcached="yes"])

  AS_IF([test "x$ac_enable_libmemcached" = "xyes"],[
    AC_LIB_HAVE_LINKFLAGS(memcached,,[
      #include <libmemcached/memcached.h>
    ],[
      memcached_st memc;
      memcached_dump_func *df;
      memcached_lib_version();
    ])
  ],[
    ac_cv_libmemcached="no"
  ])
  
  AM_CONDITIONAL(HAVE_LIBMEMCACHED, [test "x${ac_cv_libmemcached}" = "xyes"])
  
  AS_IF([test "x${ac_cv_libmemcached}" = "xyes"], [ PANDORA_WITH_MEMCACHED ])
])

AC_DEFUN([PANDORA_HAVE_LIBMEMCACHED],[
  AC_REQUIRE([_PANDORA_SEARCH_LIBMEMCACHED])
])

AC_DEFUN([PANDORA_REQUIRE_LIBMEMCACHED],[
  AC_REQUIRE([PANDORA_HAVE_LIBMEMCACHED])
  AS_IF([test x$ac_cv_libmemcached = xno],
      AC_MSG_ERROR([libmemcached is required for ${PACKAGE}]))
])
