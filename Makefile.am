ACLOCAL_AMFLAGS = -I m4 --force

lib_LTLIBRARIES = ep.la
noinst_LTLIBRARIES = libsqlite3.la

ep_la_CPPFLAGS = -I@MEMCACHED_DIR@/include $(AM_CPPFLAGS) -DSQLITE_HAS_CODEC=0
ep_la_LDFLAGS = -module -dynamic
ep_la_SOURCES = \
                 common.hh \
                 callbacks.hh \
                 ep.cc ep.hh \
                 ep_engine.cc ep_engine.h \
                 stored-value.hh flusher.hh flusher.cc \
                 kvstore.hh \
                 item.cc item.hh \
                 locks.hh \
                 mutex.hh \
                 sqlite-kvstore.cc sqlite-kvstore.hh \
                 syncobject.hh
ep_la_LIBADD = libsqlite3.la @MEMCACHED_DIR@/libmcd_util.la
ep_la_DEPENDENCIES = libsqlite3.la

libsqlite3_la_SOURCES = sqlite3.h sqlite3.c
libsqlite3_la_CFLAGS = $(AM_CFLAGS) ${NO_WERROR}

check_PROGRAMS=hash_table_test
TESTS=${check_PROGRAMS}

hash_table_test_CXXFLAGS = $(AM_CXXFLAGS) -I $(top_srcdir) ${NO_WERROR}
hash_table_test_SOURCES = t/hash_table_test.cc item.cc
hash_table_test_DEPENDENCIES = ep.hh item.hh

test: check-TESTS
