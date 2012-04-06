dnl  Copyright (C) 2012 Couchbase, Inc
dnl This file is free software; Couchbase, Inc
dnl gives unlimited permission to copy and/or distribute it,
dnl with or without modifications, as long as this notice is preserved.

AC_DEFUN([_PANDORA_SEARCH_LIBCOUCHSTORE],[
  AC_REQUIRE([AC_LIB_PREFIX])

  dnl --------------------------------------------------------------------
  dnl  Check for libcouchstore
  dnl --------------------------------------------------------------------

  AC_ARG_ENABLE([libcouchstore],
    [AS_HELP_STRING([--disable-libcouchstore],
      [Build with libcouchstore support @<:@default=on@:>@])],
    [ac_enable_libcouchstore="$enableval"],
    [ac_enable_libcouchstore="yes"])

  AS_IF([test "x$ac_enable_libcouchstore" = "xyes"],[
    AC_LIB_HAVE_LINKFLAGS(couchstore,,[
      #include <stdio.h>
      #include <libcouchstore/couch_db.h>
    ],[
      Db *db;
      couchstore_open_db(NULL, 0, &db);
    ])
  ],[
    ac_cv_libcouchstore="no"
  ])

  AM_CONDITIONAL(HAVE_LIBCOUCHSTORE, [test "x${ac_cv_libcouchstore}" = "xyes"])
])

AC_DEFUN([PANDORA_HAVE_LIBCOUCHSTORE],[
  AC_REQUIRE([_PANDORA_SEARCH_LIBCOUCHSTORE])
])

AC_DEFUN([PANDORA_REQUIRE_LIBCOUCHSTORE],[
  AC_REQUIRE([_PANDORA_SEARCH_LIBCOUCHSTORE])
  AS_IF([test "x${ac_cv_libcouchstore}" = "xno"],
    AC_MSG_ERROR([libcouchstore is required for ${PACKAGE}]))
])
