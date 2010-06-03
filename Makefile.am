ACLOCAL_AMFLAGS = -I m4 --force

lib_LTLIBRARIES = ep.la ep_testsuite.la
noinst_LTLIBRARIES = libsqlite3.la

noinst_PROGRAMS = sizes

ep_la_CPPFLAGS = -I@MEMCACHED_DIR@/include -I$(top_srcdir) $(AM_CPPFLAGS) -DSQLITE_HAS_CODEC=0
ep_la_LDFLAGS = -module -dynamic
ep_la_SOURCES = \
                 atomic.hh \
                 callbacks.hh \
                 common.hh \
                 dispatcher.cc dispatcher.hh \
                 ep.cc ep.hh \
                 ep_engine.cc ep_engine.h \
                 ep_extension.cc ep_extension.h \
                 flusher.cc flusher.hh \
                 item.cc item.hh \
                 kvstore.hh \
                 locks.hh \
                 mutex.hh \
                 priority.hh priority.cc \
                 sizes.cc \
                 sqlite-eval.hh sqlite-eval.cc \
                 sqlite-kvstore.cc sqlite-kvstore.hh \
                 sqlite-pst.hh sqlite-pst.cc \
                 sqlite-strategies.hh sqlite-strategies.cc \
                 stats.hh \
                 stored-value.hh stored-value.cc \
                 syncobject.hh \
                 tapclient.hh tapclient.cc

ep_la_LIBADD = libsqlite3.la @MEMCACHED_DIR@/libmcd_util.la
ep_la_DEPENDENCIES = libsqlite3.la

libsqlite3_la_SOURCES = embedded/sqlite3.h embedded/sqlite3.c
libsqlite3_la_CFLAGS = $(AM_CFLAGS) ${NO_WERROR}

check_PROGRAMS=atomic_test atomic_ptr_test atomic_queue_test hash_table_test priority_test vbucket_test dispatcher_test
TESTS=${check_PROGRAMS}

ep_testsuite_la_SOURCES= ep_testsuite.c
ep_testsuite_la_LDFLAGS= -module -dynamic

atomic_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
atomic_test_SOURCES = t/atomic_test.cc atomic.hh
atomic_test_DEPENDENCIES = atomic.hh

atomic_ptr_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
atomic_ptr_test_SOURCES = t/atomic_ptr_test.cc atomic.hh
atomic_ptr_test_DEPENDENCIES = atomic.hh

atomic_queue_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
atomic_queue_test_SOURCES = t/atomic_queue_test.cc atomic.hh
atomic_queue_test_DEPENDENCIES = atomic.hh

dispatcher_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
dispatcher_test_SOURCES = t/dispatcher_test.cc dispatcher.cc dispatcher.hh
dispatcher_test_DEPENDENCIES = dispatcher.hh dispatcher.cc

hash_table_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
hash_table_test_SOURCES = t/hash_table_test.cc item.cc stored-value.cc stored-value.hh
hash_table_test_DEPENDENCIES = stored-value.cc stored-value.hh ep.hh item.hh

priority_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
priority_test_SOURCES = t/priority_test.cc priority.hh priority.cc

sizes_CPPFLAGS = -I@MEMCACHED_DIR@/include -I$(top_srcdir) $(AM_CPPFLAGS) -DSQLITE_HAS_CODEC=0
sizes_SOURCES = sizes.cc
sizes_DEPENDENCIES = vbucket.hh stored-value.hh item.hh

vbucket_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
vbucket_test_SOURCES = t/vbucket_test.cc vbucket.hh stored-value.cc stored-value.hh
vbucket_test_DEPENDENCIES = vbucket.hh stored-value.cc stored-value.hh

test: all check-TESTS
	@MEMCACHED_DIR@/engine_testapp -E .libs/ep.so -T .libs/ep_testsuite.so
	./sizes