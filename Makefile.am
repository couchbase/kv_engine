ACLOCAL_AMFLAGS = -I m4 --force
bin_SCRIPTS = management/analyze_core

lib_LTLIBRARIES = ep.la ep_testsuite.la
noinst_LTLIBRARIES =

bin_PROGRAMS =

noinst_PROGRAMS = sizes

ep_la_CPPFLAGS = -I@MEMCACHED_DIR@/include -I$(top_srcdir) $(AM_CPPFLAGS) -DSQLITE_HAS_CODEC=0
ep_la_LDFLAGS = -module -dynamic
ep_la_SOURCES = \
                 atomic.hh \
                 callbacks.hh \
                 common.hh \
                 config_static.h \
                 dispatcher.cc dispatcher.hh \
                 ep.cc ep.hh \
                 ep_engine.cc ep_engine.h \
                 ep_extension.cc ep_extension.h \
                 flusher.cc flusher.hh \
                 item.cc item.hh \
                 item_pager.cc item_pager.hh \
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
                 tapconnection.hh tapconnection.cc \
                 vbucket.hh vbucket.cc


ep_la_LIBADD = @MEMCACHED_DIR@/libmcd_util.la
ep_la_DEPENDENCIES =

if BUILD_EMBEDDED_LIBSQLITE3
ep_la_LIBADD += libsqlite3.la
ep_la_DEPENDENCIES += libsqlite3.la
noinst_LTLIBRARIES += libsqlite3.la
bin_PROGRAMS += management/sqlite3
else
ep_la_LIBADD += $(LIBSQLITE3)
endif

libsqlite3_la_SOURCES = embedded/sqlite3.h embedded/sqlite3.c
libsqlite3_la_CFLAGS = $(AM_CFLAGS) ${NO_WERROR}

check_PROGRAMS=atomic_test atomic_ptr_test atomic_queue_test hash_table_test priority_test vbucket_test dispatcher_test misc_test hrtime_test
TESTS=${check_PROGRAMS}

ep_testsuite_la_CFLAGS = $(AM_CFLAGS) ${NO_WERROR}
ep_testsuite_la_SOURCES= ep_testsuite.cc
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

misc_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
misc_test_SOURCES = t/misc_test.cc common.hh
misc_test_DEPENDENCIES = common.hh

priority_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
priority_test_SOURCES = t/priority_test.cc priority.hh priority.cc

sizes_CPPFLAGS = -I@MEMCACHED_DIR@/include -I$(top_srcdir) $(AM_CPPFLAGS) -DSQLITE_HAS_CODEC=0
sizes_SOURCES = sizes.cc
sizes_DEPENDENCIES = vbucket.hh stored-value.hh item.hh

management_sqlite3_SOURCES = embedded/sqlite3-shell.c
management_sqlite3_CFLAGS = $(AM_CFLAGS) ${NO_WERROR}
management_sqlite3_DEPENDENCIES = libsqlite3.la
management_sqlite3_LDADD = libsqlite3.la

vbucket_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
vbucket_test_SOURCES = t/vbucket_test.cc vbucket.hh stored-value.cc stored-value.hh
vbucket_test_DEPENDENCIES = vbucket.hh stored-value.cc stored-value.hh

hrtime_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
hrtime_test_SOURCES = t/hrtime_test.cc common.hh

if BUILD_GETHRTIME
ep_la_SOURCES += gethrtime.c
hrtime_test_SOURCES += gethrtime.c
endif

if ENABLE_INTERNAL_TAP
ep_la_SOURCES += tapclient.cc tapclient.hh
endif

test: all check-TESTS
	@MEMCACHED_DIR@/engine_testapp -E .libs/ep.so -T .libs/ep_testsuite.so \
		-e 'ht_size=13;ht_locks=7;initfile=t/test_pragma.sql;min_data_age=0'
	./sizes
