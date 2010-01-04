dnl  Copyright (C) 2009 Sun Microsystems
dnl This file is free software; Sun Microsystems
dnl gives unlimited permission to copy and/or distribute it,
dnl with or without modifications, as long as this notice is preserved.

AC_DEFUN([PANDORA_BUILDING_FROM_VC],[

  ac_cv_building_from_vc=no

  AS_IF([test -d "${srcdir}/.bzr"],[
    ac_cv_building_from_bzr=yes
    ac_cv_building_from_vc=yes
    ],[
    ac_cv_building_from_bzr=no
  ])

  AS_IF([test -d "${srcdir}/.svn"],[
    ac_cv_building_from_svn=yes
    ac_cv_building_from_vc=yes
    ],[
    ac_cv_building_from_svn=no
  ])

  AS_IF([test -d "${srcdir}/.hg"],[
    ac_cv_building_from_hg=yes
    ac_cv_building_from_vc=yes
    ],[
    ac_cv_building_from_hg=no
  ])

  AS_IF([test -d "${srcdir}/.git"],[
    ac_cv_building_from_git=yes
    ac_cv_building_from_vc=yes
    ],[
    ac_cv_building_from_git=no
  ])


])
  
dnl Takes one argument which is the prefix to append
AC_DEFUN([PANDORA_EXPORT_BZR_INFO],[
  m4_ifval(m4_normalize([$1]),[
    m4_define([PEBI_PREFIX],[])
  ],[
    m4_define([PEBI_PREFIX],m4_toupper(m4_normalize($1))[_])
  ])

  AC_DEFINE(PEBI_PREFIX[BZR_REVID], ["BZR_REVID"], [bzr revision ID])
  AC_DEFINE(PEBI_PREFIX[BZR_BRANCH], ["BZR_BRANCH"], [bzr branch name])
  AC_DEFINE(PEBI_PREFIX[RELEASE_DATE], ["RELEASE_DATE"], [Release date based on the date of the repo checkout])
  AC_DEFINE(PEBI_PREFIX[RELEASE_VERSION], ["RELEASE_VERSION"], [$1 version number formatted for display])
  AC_DEFINE(PEBI_PREFIX[RELEASE_COMMENT], ["RELEASE_COMMENT"], [Set to trunk if the branch is the main $1 branch])
  AC_DEFINE(PEBI_PREFIX[RELEASE_ID], [RELEASE_ID], [$1 version number formatted for numerical comparison])
 
])

