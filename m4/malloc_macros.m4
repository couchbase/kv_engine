dnl  Copyright (C) 2009 Sun Microsystems
dnl This file is free software; Sun Microsystems
dnl gives unlimited permission to copy and/or distribute it,
dnl with or without modifications, as long as this notice is preserved.

AC_DEFUN([MALLOC_MACROS],[

  AC_ARG_ENABLE([tcmalloc],
    [AS_HELP_STRING([--enable-tcmalloc],
       [Enable linking with tcmalloc @<:@default=off@:>@])],
    [ac_enable_tcmalloc="$enableval"],
    [ac_enable_tcmalloc="no"])

  AC_ARG_ENABLE([tcmalloc-minimal],
    [AS_HELP_STRING([--enable-tcmalloc-minimal],
       [Enable linking with tcmalloc-minimal @<:@default=off@:>@])],
    [ac_enable_tcmalloc_minimal="$enableval"],
    [ac_enable_tcmalloc_minimal="no"])

  AC_ARG_WITH([tcmalloc-headers],
    [AS_HELP_STRING([--with-tcmalloc-headers],
      [Location of the tcmalloc header files])],
    [ac_cv_with_tcmalloc_headers="$withval"],
    [ac_cv_with_tcmalloc_headers=""])

  if test $ac_enable_tcmalloc != "no"; then
    AC_CHECK_LIB(tcmalloc, tc_malloc,
      [MALLOC_LIBS="-ltcmalloc"],
      [AC_MSG_ERROR([Unable to link with libtcmalloc"])])
    AC_DEFINE([HAVE_LIBTCMALLOC], [1], [Define if using tcmalloc])

    AS_IF(test "x${ac_cv_with_tcmalloc_headers}" != "x",
      [CPPFLAGS="-I${ac_cv_with_tcmalloc_headers} $CPPFLAGS" ])
    AC_CHECK_HEADERS(google/malloc_extension_c.h, [],
      [AC_MSG_ERROR([google/malloc_extension_c.h not found])])
    AC_CHECK_HEADERS(google/malloc_hook_c.h, [],
      [AC_MSG_ERROR([google/malloc_hook_c.h not found])])
  else
    if test $ac_enable_tcmalloc_minimal != "no"; then
      AC_CHECK_LIB(tcmalloc_minimal, tc_malloc,
        [MALLOC_LIBS="-ltcmalloc_minimal"],
        [AC_MSG_ERROR([Unable to link with libtcmalloc_minimal"])])
      AC_DEFINE([HAVE_LIBTCMALLOC_MINIMAL], [1], [Define if using tcmalloc_minimal])

      AS_IF(test "x${ac_cv_with_tcmalloc_headers}" != "x",
        [CPPFLAGS="-I${ac_cv_with_tcmalloc_headers} $CPPFLAGS" ])
      AC_CHECK_HEADERS(google/malloc_extension_c.h, [],
        [AC_MSG_ERROR([google/malloc_extension_c.h not found])])
      AC_CHECK_HEADERS(google/malloc_hook_c.h, [],
        [AC_MSG_ERROR([google/malloc_hook_c.h not found])])
    fi
  fi

  AC_SUBST([MALLOC_LIBS])
])
