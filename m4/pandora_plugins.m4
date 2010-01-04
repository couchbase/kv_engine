dnl  Copyright (C) 2009 Sun Microsystems
dnl This file is free software; Sun Microsystems
dnl gives unlimited permission to copy and/or distribute it,
dnl with or without modifications, as long as this notice is preserved.
dnl--------------------------------------------------------------------
dnl PANDORA_PLUGINS
dnl Declare our plugin modules
dnl--------------------------------------------------------------------

AC_DEFUN([PANDORA_PLUGINS],[

  m4_sinclude(config/plugin.ac)
  dnl Add code here to read set plugin lists and  set drizzled_default_plugin_list
  AC_DEFINE_UNQUOTED([PANDORA_PLUGIN_LIST],[$pandora_default_plugin_list],
                     [List of plugins that should be loaded on startup if no
                      value is given for --plugin-load])

  pandora_builtin_list=`echo $pandora_builtin_list | sed 's/, *$//'`
  AS_IF([test "x$pandora_builtin_list" = "x"], pandora_builtin_list="NULL")
  AC_SUBST([PANDORA_BUILTIN_LIST],[$pandora_builtin_list])
  m4_ifval(m4_normalize([$1]),[
    AC_CONFIG_FILES($*)
    ],[
    AC_DEFINE_UNQUOTED([PANDORA_BUILTIN_LIST],[$pandora_builtin_list],
                       [List of plugins to be built in])
  ])


  AC_SUBST(pandora_plugin_test_list)
  AC_SUBST(pandora_plugin_libs)

  pandora_plugin_defs=`echo $pandora_plugin_defs | sed 's/, *$//'`
  AC_SUBST(pandora_plugin_defs)

  AC_SUBST(PANDORA_PLUGIN_DEP_LIBS)
  AC_SUBST(pkgplugindir,"\$(pkglibdir)/plugin")
])

AC_DEFUN([PANDORA_ADD_PLUGIN_DEP_LIB],[
  PANDORA_PLUGIN_DEP_LIBS="${PANDORA_PLUGIN_DEP_LIBS} $*"
])
