ACLOCAL_AMFLAGS = -I m4 --force
bin_SCRIPTS = management/analyze_core \
              wrapper/dbmaint \
              wrapper/flushctl \
              wrapper/stats \
              wrapper/vbucketctl

lib_LTLIBRARIES = ep.la ep_testsuite.la timing_tests.la
noinst_LTLIBRARIES =

bin_PROGRAMS =
BUILT_SOURCES =
CLEANFILES =

EXTRA_DIST = docs management README.markdown win32 Doxyfile LICENSE

noinst_PROGRAMS = sizes

ep_la_CPPFLAGS = -I$(top_srcdir) $(AM_CPPFLAGS)
ep_la_LDFLAGS = -module -dynamic
ep_la_SOURCES = \
                 atomic/gcc_atomics.h \
                 atomic/libatomic.h \
                 atomic.hh \
                 callbacks.hh \
                 command_ids.h \
                 common.hh \
                 config_static.h \
                 dispatcher.cc dispatcher.hh \
                 ep.cc ep.hh \
                 ep_engine.cc ep_engine.h \
                 ep_extension.cc ep_extension.h \
                 flusher.cc flusher.hh \
                 histo.hh \
                 item.cc item.hh \
                 item_pager.cc item_pager.hh \
                 locks.hh \
                 mutex.hh \
                 priority.cc priority.hh \
                 queueditem.hh \
                 ringbuffer.hh \
                 sizes.cc \
                 sqlite-eval.cc sqlite-eval.hh \
                 sqlite-kvstore.cc sqlite-kvstore.hh \
                 sqlite-pst.cc sqlite-pst.hh \
                 sqlite-strategies.cc sqlite-strategies.hh \
                 stats.hh \
                 statsnap.cc statsnap.hh \
                 stored-value.cc stored-value.hh \
                 syncobject.hh \
                 tapconnection.cc tapconnection.hh \
                 tapconnmap.cc tapconnmap.hh \
                 vbucket.cc vbucket.hh

if BUILD_BYTEORDER
ep_la_SOURCES += byteorder.c
endif

ep_la_LIBADD =
ep_la_DEPENDENCIES =
ep_testsuite_la_LIBADD =
ep_testsuite_la_DEPENDENCIES =

if BUILD_EMBEDDED_LIBSQLITE3
ep_la_LIBADD += libsqlite3.la
ep_la_DEPENDENCIES += libsqlite3.la
ep_testsuite_la_LIBADD += libsqlite3.la
ep_testsuite_la_DEPENDENCIES += libsqlite3.la
noinst_LTLIBRARIES += libsqlite3.la
bin_PROGRAMS += management/sqlite3
else
ep_la_LIBADD += $(LIBSQLITE3)
ep_testsuite_la_LIBADD += $(LIBSQLITE3)
endif

libsqlite3_la_SOURCES = embedded/sqlite3.h embedded/sqlite3.c
libsqlite3_la_CFLAGS = $(AM_CFLAGS) ${NO_WERROR} -DSQLITE_THREADSAFE=2

check_PROGRAMS=atomic_test atomic_ptr_test atomic_queue_test hash_table_test priority_test vbucket_test dispatcher_test misc_test hrtime_test histo_test chunk_creation_test ringbuffer_test

TESTS=${check_PROGRAMS}
EXTRA_TESTS =

ep_testsuite_la_CFLAGS = $(AM_CFLAGS) ${NO_WERROR}
ep_testsuite_la_SOURCES= ep_testsuite.cc ep_testsuite.h \
                         sqlite-pst.hh sqlite-pst.cc locks.hh mutex.hh
ep_testsuite_la_LDFLAGS= -module -dynamic

if BUILD_GENERATED_TESTS
lib_LTLIBRARIES += generated_suite.la

generated_suite.c: t/gen_engine_test.py t/testgen.py
	${top_srcdir}/t/gen_engine_test.py > generated_suite.c || ( rm generated_suite.c && /bin/false)
BUILT_SOURCES+= generated_suite.c
CLEANFILES+= generated_suite.c

generated_suite_la_DEPENDENCIES = ep.la
generated_suite_la_CFLAGS = $(AM_CFLAGS) ${NO_WERROR} -O0
generated_suite_la_SOURCES= generated_suite.c testsuite.h suite_stubs.c ep_testsuite.h
generated_suite_la_LDFLAGS= -module -dynamic

EXTRA_TESTS += generated_engine_tests

generated_engine_tests: generated_suite.la
	$(ENGINE_TESTAPP) -E .libs/ep.so -t $(TEST_TIMEOUT) \
		-. -q -T .libs/generated_suite.so \
		-e 'ht_size=13;ht_locks=7;db_strategy=singleDB;dbname=:memory:'
endif

timing_tests_la_CFLAGS = $(AM_CFLAGS) ${NO_WERROR}
timing_tests_la_SOURCES= timing_tests.cc
timing_tests_la_LDFLAGS= -module -dynamic

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
dispatcher_test_SOURCES = t/dispatcher_test.cc dispatcher.cc dispatcher.hh priority.cc priority.hh
dispatcher_test_DEPENDENCIES = common.hh dispatcher.hh dispatcher.cc priority.cc priority.hh

hash_table_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
hash_table_test_SOURCES = t/hash_table_test.cc item.cc stored-value.cc stored-value.hh
hash_table_test_DEPENDENCIES = stored-value.cc stored-value.hh ep.hh item.hh

misc_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
misc_test_SOURCES = t/misc_test.cc common.hh
misc_test_DEPENDENCIES = common.hh

priority_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
priority_test_SOURCES = t/priority_test.cc priority.hh priority.cc

sizes_CPPFLAGS = -I$(top_srcdir) $(AM_CPPFLAGS)
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

histo_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
histo_test_SOURCES = t/histo_test.cc common.hh histo.hh
histo_test_DEPENDENCIES = common.hh histo.hh

chunk_creation_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
chunk_creation_test_SOURCES = t/chunk_creation_test.cc common.hh

ringbuffer_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
ringbuffer_test_SOURCES = t/ringbuffer_test.cc ringbuffer.hh
ringbuffer_test_DEPENDENCIES = ringbuffer.hh

if BUILD_GETHRTIME
ep_la_SOURCES += gethrtime.c
hrtime_test_SOURCES += gethrtime.c
dispatcher_test_SOURCES += gethrtime.c
endif

pythonlibdir=$(libdir)/python
pythonlib_DATA= \
                management/capture.py \
                management/clitool.py \
                management/mc_bin_client.py \
                management/mc_bin_server.py \
                management/memcacheConstants.py \
                management/tap.py

pythonlib_SCRIPTS= \
                management/dbmaint \
                management/flushctl \
                management/stats \
                management/vbucketctl

PYTHON_TOOLS= wrapper/dbmaint \
              wrapper/flushctl \
              wrapper/stats \
              wrapper/vbucketctl

${PYTHON_TOOLS}: wrapper/wrapper
	cp $< $@

BUILT_SOURCES += ${PYTHON_TOOLS}
CLEANFILES+= ${PYTHON_TOOLS}

TEST_TIMEOUT=30

engine_tests: all
	$(ENGINE_TESTAPP) -E .libs/ep.so -t $(TEST_TIMEOUT) \
		-T .libs/ep_testsuite.so \
		-e 'ht_size=13;ht_locks=7;initfile=t/test_pragma.sql;min_data_age=0'

test: all check-TESTS engine_tests sizes
	./sizes
