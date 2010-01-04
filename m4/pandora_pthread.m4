dnl -*- mode: m4; c-basic-offset: 2; indent-tabs-mode: nil; -*-
dnl vim:expandtab:shiftwidth=2:tabstop=2:smarttab:
dnl   
dnl pandora-build: A pedantic build system
dnl Copyright (C) 2009 Sun Microsystems, Inc.
dnl This file is free software; Sun Microsystems
dnl gives unlimited permission to copy and/or distribute it,
dnl with or without modifications, as long as this notice is preserved.
dnl
dnl From Monty Taylor


dnl --------------------------------------------------------------------
dnl  Check for libpthread
dnl --------------------------------------------------------------------

AC_DEFUN([PANDORA_PTHREAD_YIELD],[
  AC_REQUIRE([ACX_PTHREAD])

  save_CFLAGS="${CFLAGS}"
  save_CXXFLAGS="${CXXFLAGS}"
  CFLAGS="${PTHREAD_CFLAGS} ${CFLAGS}"
  CXXFLAGS="${PTHREAD_CFLAGS} ${CXXFLAGS}"
  dnl Some OSes like Mac OS X have that as a replacement for pthread_yield()
  AC_CHECK_FUNCS(pthread_yield_np)
  AC_CACHE_CHECK([if pthread_yield takes zero arguments],
    [pandora_cv_pthread_yield_zero_arg],
    [AC_LINK_IFELSE([
      AC_LANG_PROGRAM([[
#include <pthread.h>
        ]],[[
  pthread_yield();
        ]])],
      [pandora_cv_pthread_yield_zero_arg=yes],
      [pandora_cv_pthread_yield_zero_arg=no])])
  AS_IF([test "$pandora_cv_pthread_yield_zero_arg" = "yes"],[
    AC_DEFINE([HAVE_PTHREAD_YIELD_ZERO_ARG], [1],
              [pthread_yield that doesn't take any arguments])
  ])

  AC_CACHE_CHECK([if pthread_yield takes one argument],
    [pandora_cv_pthread_yield_one_arg],
    [AC_LINK_IFELSE([
      AC_LANG_PROGRAM([[
#include <pthread.h>
        ]],[[
  pthread_yield(0);
        ]])],
      [pandora_cv_pthread_yield_one_arg=yes],
      [pandora_cv_pthread_yield_one_arg=no])])
  AS_IF([test "$pandora_cv_pthread_yield_one_arg" = "yes"],[
    AC_DEFINE([HAVE_PTHREAD_YIELD_ONE_ARG], [1],
              [pthread_yield function with one argument])
  ])

  CFLAGS="${save_CFLAGS}"
  CXXFLAGS="${save_CXXFLAGS}"
])


AC_DEFUN([_PANDORA_SEARCH_PTHREAD],[
  AC_REQUIRE([ACX_PTHREAD])
  LIBS="${PTHREAD_LIBS} ${LIBS}"
  AM_CFLAGS="${PTHREAD_CFLAGS} ${AM_CFLAGS}"
  AM_CXXFLAGS="${PTHREAD_CFLAGS} ${AM_CXXFLAGS}"
  PANDORA_PTHREAD_YIELD
])


AC_DEFUN([PANDORA_HAVE_PTHREAD],[
  AC_REQUIRE([_PANDORA_SEARCH_PTHREAD])
])

AC_DEFUN([PANDORA_REQUIRE_PTHREAD],[
  AC_REQUIRE([PANDORA_HAVE_PTHREAD])
  AS_IF([test "x$acx_pthread_ok" != "xyes"],[
    AC_MSG_ERROR(could not find libpthread)])
])
