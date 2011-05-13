dnl  Copyright (C) 2009 Sun Microsystems
dnl This file is free software; Sun Microsystems
dnl gives unlimited permission to copy and/or distribute it,
dnl with or without modifications, as long as this notice is preserved.

dnl We check two things: where is unordered_map's include file, and in what
dnl namespace does unordered_map reside.
dnl We include AC_COMPILE_IFELSE for all the combinations we've seen in the
dnl wild:
dnl 
dnl  GCC 4.2: namespace: tr1::  #include <tr1/unordered_map> 
dnl  GCC 4.2: namespace: boost::  #include <boost/unordered_map.hpp> 
dnl
dnl We define one of HAVE_TR1_UNORDERED_MAP or HAVE_BOOST_UNORDERED_MAP
dnl depending on location, and UNORDERED_MAP_NAMESPACE to be the namespace in
dnl which unordered_map is defined.
dnl 

AC_DEFUN([PANDORA_UNORDERED_MAP],[
  AC_REQUIRE([PANDORA_CHECK_CXX_STANDARD])
  AC_LANG_PUSH(C++)
  AC_CHECK_HEADERS(tr1/unordered_map boost/unordered_map.hpp)
  AC_CACHE_CHECK([the location of unordered_map header file],
    [ac_cv_unordered_map_h],[
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
  AC_LANG_POP()
])
