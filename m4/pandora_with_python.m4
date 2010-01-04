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


AC_DEFUN([PANDORA_WITH_PYTHON], [

  AC_ARG_WITH([python], 
    [AS_HELP_STRING([--with-python],
      [Build Python Bindings @<:@default=yes@:>@])],
    [with_python=$withval], 
    [with_python=yes])

  AS_IF([test "x$with_python" != "xno"],[
    AS_IF([test "x$with_python" != "xyes"],[PYTHON=$with_python])
    AM_PATH_PYTHON([2.4],,[with_python="no"])
    AC_PYTHON_DEVEL()
    AS_IF([test "x$pythonexists" = "xno"],[with_python="no"])
  ])
  AM_CONDITIONAL(BUILD_PYTHON, [test "$with_python" = "yes"])
])
