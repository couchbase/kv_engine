ACLOCAL_AMFLAGS = -I m4 --force

lib_LTLIBRARIES = ep.la
noinst_LTLIBRARIES = libsqlite3.la

ep_la_CPPFLAGS = -I@MEMCACHED_DIR@/include -I$(top_srcdir) $(AM_CPPFLAGS) -DSQLITE_HAS_CODEC=0
ep_la_LDFLAGS = -module -dynamic
ep_la_SOURCES = \
                 callbacks.hh \
                 common.hh \
                 ep.cc ep.hh \
                 ep_engine.cc ep_engine.h \
                 flusher.cc flusher.hh \
                 item.cc item.hh \
                 kvstore.hh \
                 locks.hh \
                 mutex.hh \
                 priority.hh priority.cc \
                 sqlite-eval.hh sqlite-eval.cc \
                 sqlite-kvstore.cc sqlite-kvstore.hh \
                 sqlite-pst.hh sqlite-pst.cc \
                 sqlite-strategies.hh sqlite-strategies.cc \
                 stored-value.hh \
                 syncobject.hh

ep_la_LIBADD = libsqlite3.la @MEMCACHED_DIR@/libmcd_util.la
ep_la_DEPENDENCIES = libsqlite3.la

libsqlite3_la_SOURCES = embedded/sqlite3.h embedded/sqlite3.c
libsqlite3_la_CFLAGS = $(AM_CFLAGS) ${NO_WERROR}

check_PROGRAMS=hash_table_test priority_test
TESTS=${check_PROGRAMS}

hash_table_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
hash_table_test_SOURCES = t/hash_table_test.cc item.cc
hash_table_test_DEPENDENCIES = ep.hh item.hh

priority_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
priority_test_SOURCES = t/priority_test.cc priority.hh priority.cc

test: check-TESTS
