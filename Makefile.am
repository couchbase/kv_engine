ACLOCAL_AMFLAGS = -I m4 --force
bin_SCRIPTS = management/analyze_core \
              management/squasher.sql \
              wrapper/cbadm-online-restore \
              wrapper/cbadm-online-update \
              wrapper/cbadm-tap-registration \
              wrapper/cbbackup-incremental \
              wrapper/cbbackup-merge-incremental \
              wrapper/cbdbmaint \
              wrapper/cbdbupgrade \
              wrapper/cbconfig \
              wrapper/cbstats \
              wrapper/cbvbucketctl

pkginclude_HEADERS = command_ids.h


memcachedlibdir = $(libdir)/memcached
memcachedlib_LTLIBRARIES = ep.la ep_testsuite.la timing_tests.la
noinst_LTLIBRARIES = \
                     libblackhole-kvstore.la \
                     libconfiguration.la \
                     libkvstore.la \
                     libmc-kvstore.la \
                     libobjectregistry.la \
                     libsqlite-kvstore.la \
                     libcouch-kvstore.la \
                     libdirutils.la

fixup_stats_json:
	cat docs/stats.json | python -mjson.tool > /tmp/stats.json.tmp
	mv /tmp/stats.json.tmp docs/stats.json

dist_doc_DATA = docs/stats.json

bin_PROGRAMS = management/cbdbconvert
BUILT_SOURCES = generated_configuration.cc \
                generated_configuration.hh \
                stats-info.c stats-info.h
CLEANFILES = mbbackup-0001.mbb mbbackup-0002.mbb

EXTRA_DIST = Doxyfile LICENSE README.markdown configuration.json docs \
             dtrace management win32

noinst_PROGRAMS = sizes gen_config gen_code

man_MANS =
if BUILD_DOCS
man_MANS += cbadm-online-update.1m \
            cbbackup-incremental.1m
endif

cbadm-online-update.1m: docs/cbadm-online-update.pod
	${POD2MAN} -c "$*" -r "" -s 1m $< $@

cbbackup-incremental.1m: docs/cbbackup-incremental.pod
	${POD2MAN} -c "$*" -r "" -s 1m $< $@

ep_la_CPPFLAGS = -I$(top_srcdir) $(AM_CPPFLAGS)
ep_la_LDFLAGS = -module -dynamic
ep_la_SOURCES = \
                 access_scanner.cc \
                 access_scanner.hh \
                 atomic/gcc_atomics.h \
                 atomic/libatomic.h \
                 atomic.cc atomic.hh \
                 backfill.hh \
                 backfill.cc \
                 callbacks.hh \
                 checkpoint.hh \
                 checkpoint.cc \
                 checkpoint_remover.hh \
                 checkpoint_remover.cc \
                 command_ids.h \
                 common.hh \
                 config_static.h \
                 dispatcher.cc dispatcher.hh \
                 ep.cc ep.hh \
                 ep_engine.cc ep_engine.h \
                 ep_extension.cc ep_extension.h \
                 ep_time.c ep_time.h \
                 flusher.cc flusher.hh \
                 histo.hh \
                 htresizer.cc htresizer.hh \
                 invalid_vbtable_remover.hh \
                 invalid_vbtable_remover.cc \
                 item.cc item.hh \
                 item_pager.cc item_pager.hh \
                 kvstore.hh \
                 locks.hh \
                 memory_tracker.cc memory_tracker.hh \
                 mutex.cc mutex.hh \
                 priority.cc priority.hh \
                 queueditem.cc queueditem.hh \
                 restore.hh \
                 restore_impl.cc \
                 ringbuffer.hh \
                 sizes.cc \
                 stats.hh \
                 stats-info.h stats-info.c \
                 statsnap.cc statsnap.hh \
                 statwriter.hh \
                 stored-value.cc stored-value.hh \
                 syncobject.hh \
                 tapconnection.cc tapconnection.hh \
                 tapconnmap.cc tapconnmap.hh \
                 tapthrottle.cc tapthrottle.hh \
                 vbucket.cc vbucket.hh \
                 vbucketmap.cc vbucketmap.hh \
                 warmup.cc warmup.hh


libobjectregistry_la_SOURCES = objectregistry.cc objectregistry.hh

libkvstore_la_SOURCES = crc32.c crc32.h kvstore.cc kvstore.hh   \
                        mutation_log.cc mutation_log.hh         \
                        mutation_log_compactor.cc               \
                        mutation_log_compactor.hh
libkvstore_la_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/sqlite-kvstore \
                         $(AM_CPPFLAGS)

libsqlite_kvstore_la_SOURCES = kvstore.hh \
                               sqlite-kvstore/factory.cc \
                               sqlite-kvstore/pathexpand.hh \
                               sqlite-kvstore/pathexpand.cc \
                               sqlite-kvstore/sqlite-eval.cc \
                               sqlite-kvstore/sqlite-eval.hh \
                               sqlite-kvstore/sqlite-kvstore.cc \
                               sqlite-kvstore/sqlite-kvstore.hh \
                               sqlite-kvstore/sqlite-pst.cc \
                               sqlite-kvstore/sqlite-pst.hh \
                               sqlite-kvstore/sqlite-strategies.cc \
                               sqlite-kvstore/sqlite-strategies.hh \
                               sqlite-kvstore/sqlite-vfs.c
libsqlite_kvstore_la_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/sqlite-kvstore \
                                $(AM_CPPFLAGS)
libsqlite_kvstore_la_LIBADD =
libsqlite_kvstore_la_DEPENDENCIES =

libblackhole_kvstore_la_SOURCES = kvstore.hh \
                                  blackhole-kvstore/blackhole.cc \
                                  blackhole-kvstore/blackhole.hh

libmc_kvstore_la_SOURCES = kvstore.hh \
                           mc-kvstore/mc-engine.cc \
                           mc-kvstore/mc-engine.hh \
                           mc-kvstore/mc-kvstore.cc \
                           mc-kvstore/mc-kvstore.hh \
                           tools/cJSON.c \
                           tools/cJSON.h


libmc_kvstore_la_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/sqlite-kvstore \
                            $(AM_CPPFLAGS)
libmc_kvstore_la_LIBADD =
libmc_kvstore_la_DEPENDENCIES =

.generated_stat-info: gen_code docs/stats.json
	./gen_code -j docs/stats.json -h stats-info.h -c stats-info.c -f get_stats_info && touch .generated_stat-info

stats-info.h stats-info.c: .generated_stat-info

.generated_configuration: gen_config configuration.json
	./gen_config && touch .generated_configuration

generated_configuration.hh generated_configuration.cc: .generated_configuration

libdirutils_la_SOURCES = couch-kvstore/dirutils.cc \
                         couch-kvstore/dirutils.hh


libcouch_kvstore_la_SOURCES = kvstore.hh

if HAVE_LIBCOUCHSTORE
libcouch_kvstore_la_SOURCES += couch-kvstore/couch-kvstore.cc \
                               couch-kvstore/couch-kvstore.hh
libcouch_kvstore_la_LIBADD = libdirutils.la $(LTLIBCOUCHSTORE)
else
libcouch_kvstore_la_SOURCES += couch-kvstore/couch-kvstore-dummy.hh \
                               couch-kvstore/couch-kvstore-dummy.cc
endif


libconfiguration_la_SOURCES = \
                              generated_configuration.hh \
                              configuration.hh \
                              configuration.cc

ep_la_LIBADD = libkvstore.la libsqlite-kvstore.la libmc-kvstore.la \
               libblackhole-kvstore.la libcouch-kvstore.la \
               libobjectregistry.la libconfiguration.la $(LTLIBEVENT)
ep_la_DEPENDENCIES = libkvstore.la libsqlite-kvstore.la	\
               libmc-kvstore.la libblackhole-kvstore.la	\
               libobjectregistry.la libconfiguration.la \
               libcouch-kvstore.la
ep_testsuite_la_LIBADD =libobjectregistry.la $(LTLIBEVENT)
ep_testsuite_la_DEPENDENCIES = libobjectregistry.la

management_cbdbconvert_SOURCES = atomic.cc mutex.cc                     \
                                 management/dbconvert.cc testlogger.cc  \
                                 item.cc
management_cbdbconvert_LDADD = libkvstore.la libsqlite-kvstore.la       \
                               libmc-kvstore.la                         \
                               libblackhole-kvstore.la                  \
                               libobjectregistry.la                     \
                               libconfiguration.la libkvstore.la        \
                               libblackhole-kvstore.la                  \
                               libcouch-kvstore.la                      \
                               libmc-kvstore.la $(LTLIBEVENT)
management_cbdbconvert_DEPENDENCIES = libkvstore.la libsqlite-kvstore.la libmc-kvstore.la \
                                      libcouch-kvstore.la libobjectregistry.la libconfiguration.la

if BUILD_EMBEDDED_LIBSQLITE3
ep_la_LIBADD += libsqlite3.la
ep_la_DEPENDENCIES += libsqlite3.la
ep_testsuite_la_LIBADD += libsqlite3.la
ep_testsuite_la_DEPENDENCIES += libsqlite3.la
management_cbdbconvert_LDADD += libsqlite3.la
management_cbdbconvert_DEPENDENCIES += libsqlite3.la
noinst_LTLIBRARIES += libsqlite3.la
bin_PROGRAMS += management/sqlite3
else
ep_la_LIBADD += $(LIBSQLITE3)
ep_testsuite_la_LIBADD += $(LIBSQLITE3)
management_cbdbconvert_LDADD += $(LIBSQLITE3)
endif

libsqlite3_la_SOURCES = embedded/sqlite3.h embedded/sqlite3.c
libsqlite3_la_CFLAGS = $(AM_CFLAGS) ${NO_WERROR} -DSQLITE_THREADSAFE=2

check_PROGRAMS=\
               atomic_ptr_test \
               atomic_test \
               checkpoint_test \
               chunk_creation_test \
               dispatcher_test \
               hash_table_test \
               histo_test \
               hrtime_test \
               misc_test \
               mutation_log_test \
               mutex_test \
               pathexpand_test \
               priority_test \
               ringbuffer_test \
               vbucket_test

if HAVE_GOOGLETEST
check_PROGRAMS += t/dirutils_test
endif

TESTS=${check_PROGRAMS}
EXTRA_TESTS =

ep_testsuite_la_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/sqlite-kvstore \
                         $(AM_CPPFLAGS) ${NO_WERROR}
ep_testsuite_la_SOURCES= ep_testsuite.cc ep_testsuite.h atomic.cc       \
                         locks.hh mutex.cc mutex.hh item.cc             \
                         testlogger_libify.cc dispatcher.cc ep_time.c   \
                         ep_time.h sqlite-kvstore/sqlite-pst.cc         \
                         tools/cJSON.c tools/cJSON.h                    \
                         mock/mccouch.cc mock/mccouch.hh
ep_testsuite_la_LDFLAGS= -module -dynamic

# This is because automake can't figure out how to build the same code
# for two different targets.
testlogger_libify.cc: testlogger.cc
	cp $(top_srcdir)/testlogger.cc testlogger_libify.cc

if BUILD_GENERATED_TESTS
memcachedlib_LTLIBRARIES += generated_suite.la

.genstamp: t/gen_engine_test.py t/breakdancer.py
	${top_srcdir}/t/gen_engine_test.py
	touch .genstamp

GEN_FILES=generated_suite_0.c \
          generated_suite_1.c \
          generated_suite_2.c \
          generated_suite_3.c \
          generated_suite_4.c \
          generated_suite_5.c \
          generated_suite_6.c \
          generated_suite_7.c \
          generated_suite_8.c \
          generated_suite_9.c


$(GEN_FILES): .genstamp suite_stubs.h
BUILT_SOURCES+= $(GEN_FILES)
CLEANFILES+= $(GEN_FILES) .genstamp

generated_suite_la_DEPENDENCIES = ep.la
generated_suite_la_CFLAGS = $(AM_CFLAGS) -O0
nodist_generated_suite_la_SOURCES = $(GEN_FILES)
generated_suite_la_SOURCES= suite_stubs.c ep_testsuite.h
generated_suite_la_LDFLAGS= -module -dynamic

EXTRA_TESTS += generated_engine_tests

GENERATED_TEST_TIMEOUT=5

generated_engine_tests: generated_suite.la
	$(ENGINE_TESTAPP) -E .libs/ep.so -t $(GENERATED_TEST_TIMEOUT) \
		-. -q -T .libs/generated_suite.so \
		-e 'flushall_enabled=true;ht_size=13;ht_locks=7;backend=blackhole'
endif

timing_tests_la_CFLAGS = $(AM_CFLAGS) ${NO_WERROR}
timing_tests_la_SOURCES= timing_tests.cc
timing_tests_la_LDFLAGS= -module -dynamic

atomic_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
atomic_test_SOURCES = t/atomic_test.cc atomic.hh mutex.cc
atomic_test_DEPENDENCIES = atomic.hh

atomic_ptr_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
atomic_ptr_test_SOURCES = t/atomic_ptr_test.cc atomic.cc atomic.hh mutex.cc mutex.hh
atomic_ptr_test_DEPENDENCIES = atomic.hh

mutex_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
mutex_test_SOURCES = t/mutex_test.cc locks.hh mutex.cc
mutex_test_DEPENDENCIES = locks.hh

dispatcher_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
dispatcher_test_SOURCES = t/dispatcher_test.cc dispatcher.cc	\
                          dispatcher.hh priority.cc priority.hh	\
                          testlogger.cc atomic.cc mutex.cc
dispatcher_test_DEPENDENCIES = common.hh dispatcher.hh dispatcher.cc	\
                               priority.cc priority.hh libobjectregistry.la
dispatcher_test_LDADD = libobjectregistry.la

hash_table_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
hash_table_test_SOURCES = t/hash_table_test.cc item.cc stored-value.cc	\
                          stored-value.hh testlogger.cc atomic.cc mutex.cc \
                          tools/cJSON.c test_memory_tracker.cc memory_tracker.hh
hash_table_test_DEPENDENCIES = stored-value.cc stored-value.hh ep.hh item.hh \
                               libobjectregistry.la
hash_table_test_LDADD = libobjectregistry.la

misc_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
misc_test_SOURCES = t/misc_test.cc common.hh
misc_test_DEPENDENCIES = common.hh

priority_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
priority_test_SOURCES = t/priority_test.cc priority.hh priority.cc

sizes_CPPFLAGS = -I$(top_srcdir) $(AM_CPPFLAGS)
sizes_SOURCES = sizes.cc
sizes_DEPENDENCIES = vbucket.hh stored-value.hh item.hh

gen_config_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/tools $(AM_CPPFLAGS)
gen_config_SOURCES = tools/genconfig.cc tools/cJSON.c tools/cJSON.h
gen_config_LDFLAGS = -lm

gen_code_CPPFLAGS = -I$(top_srcdir) -I$(top_srcdir)/tools $(AM_CPPFLAGS)
gen_code_SOURCES = tools/gencode.cc tools/cJSON.c tools/cJSON.h

management_sqlite3_SOURCES = embedded/sqlite3-shell.c
management_sqlite3_CFLAGS = $(AM_CFLAGS) ${NO_WERROR}
management_sqlite3_DEPENDENCIES = libsqlite3.la
management_sqlite3_LDADD = libsqlite3.la

vbucket_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
vbucket_test_SOURCES = t/vbucket_test.cc t/threadtests.hh vbucket.hh	\
               vbucket.cc stored-value.cc stored-value.hh atomic.cc	\
               testlogger.cc checkpoint.hh checkpoint.cc byteorder.c    \
               mutex.cc vbucketmap.cc test_memory_tracker.cc memory_tracker.hh \
               item.cc tools/cJSON.c
vbucket_test_DEPENDENCIES = vbucket.hh stored-value.cc stored-value.hh  \
               checkpoint.hh checkpoint.cc libobjectregistry.la         \
               libconfiguration.la
vbucket_test_LDADD = libobjectregistry.la libconfiguration.la

checkpoint_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
checkpoint_test_SOURCES = t/checkpoint_test.cc checkpoint.hh            \
                          checkpoint.cc vbucket.hh vbucket.cc           \
                          testlogger.cc stored-value.cc                 \
                          stored-value.hh queueditem.hh byteorder.c     \
                          atomic.cc mutex.cc test_memory_tracker.cc     \
                          memory_tracker.hh item.cc tools/cJSON.c
checkpoint_test_DEPENDENCIES = checkpoint.hh vbucket.hh         \
              stored-value.cc stored-value.hh queueditem.hh     \
              libobjectregistry.la libconfiguration.la
checkpoint_test_LDADD = libobjectregistry.la libconfiguration.la

t_dirutils_test_SOURCES = t/dirutils_test.cc
t_dirutils_test_DEPENDENCIES = libdirutils.la
t_dirutils_test_LDADD = libdirutils.la
t_dirutils_test_LDFLAGS = -lgtest

mutation_log_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
mutation_log_test_SOURCES = t/mutation_log_test.cc mutation_log.hh	\
                            testlogger.cc mutation_log.cc \
                            byteorder.c \
                            crc32.h crc32.c
mutation_log_test_DEPENDENCIES = mutation_log.hh
mutation_log_test_LDADD =

hrtime_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
hrtime_test_SOURCES = t/hrtime_test.cc common.hh

histo_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
histo_test_SOURCES = t/histo_test.cc common.hh histo.hh
histo_test_DEPENDENCIES = common.hh histo.hh

chunk_creation_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
chunk_creation_test_SOURCES = t/chunk_creation_test.cc common.hh

pathexpand_test_CPPFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) \
                           -I$(top_srcdir)/sqlite-kvstore ${NO_WERROR}
pathexpand_test_SOURCES = t/pathexpand_test.cc          \
                          sqlite-kvstore/pathexpand.hh  \
                          sqlite-kvstore/pathexpand.cc

ringbuffer_test_CXXFLAGS = $(AM_CXXFLAGS) -I$(top_srcdir) ${NO_WERROR}
ringbuffer_test_SOURCES = t/ringbuffer_test.cc ringbuffer.hh
ringbuffer_test_DEPENDENCIES = ringbuffer.hh

if BUILD_GETHRTIME
ep_la_SOURCES += gethrtime.c
hrtime_test_SOURCES += gethrtime.c
dispatcher_test_SOURCES += gethrtime.c
vbucket_test_SOURCES += gethrtime.c
checkpoint_test_SOURCES += gethrtime.c
management_cbdbconvert_SOURCES += gethrtime.c
ep_testsuite_la_SOURCES += gethrtime.c
hash_table_test_SOURCES += gethrtime.c
mutation_log_test_SOURCES += gethrtime.c
endif

if BUILD_BYTEORDER
ep_la_SOURCES += byteorder.c
ep_testsuite_la_SOURCES += byteorder.c
management_cbdbconvert_SOURCES += byteorder.c
endif

pythonlibdir=$(libdir)/python
pythonlib_DATA= \
                management/capture.py \
                management/clitool.py \
                management/mc_bin_client.py \
                management/mc_bin_server.py \
                management/memcacheConstants.py \
                management/tap.py \
                management/tap_example.py \
                management/util.py \
                management/backup_util.py

pythonlib_SCRIPTS= \
                management/cbadm-online-restore \
                management/cbadm-online-update \
                management/cbadm-tap-registration \
                management/cbbackup-incremental \
                management/cbbackup-merge-incremental \
                management/cbdbmaint \
                management/cbdbupgrade \
                management/cbconfig \
                management/cbstats \
                management/cbvbucketctl

PYTHON_TOOLS= \
              wrapper/cbadm-online-restore \
              wrapper/cbadm-online-update \
              wrapper/cbadm-tap-registration \
              wrapper/cbbackup-incremental \
              wrapper/cbbackup-merge-incremental \
              wrapper/cbdbmaint \
              wrapper/cbdbupgrade \
              wrapper/cbconfig \
              wrapper/cbstats \
              wrapper/cbvbucketctl

${PYTHON_TOOLS}: wrapper/wrapper
	cp $< $@

BUILT_SOURCES += ${PYTHON_TOOLS}
CLEANFILES+= ${PYTHON_TOOLS}

TEST_TIMEOUT=60

mbbackup-0002.mbb: $(top_srcdir)/t/generate-mbbackup
	echo "Generate incremental restore files used by the test suite"
	@$(top_srcdir)/t/generate-mbbackup

VALGRIND=valgrind
VALGRIND_OPTIONS=--log-file=valgrind.log --tool=Memcheck --num-callers=50 \
                 --error-limit=yes --track-origins=yes --malloc-fill=55 \
                 --free-fill=aa --dsymutil=yes -q

valgrind_engine_tests: ep.la ep_testsuite.la mbbackup-0002.mbb
	@$(MAKE) \
             ENGINE_TESTAPP_LAUNCHER="$(VALGRIND) $(VALGRIND_OPTIONS)" \
             engine_tests

engine_tests: ep.la ep_testsuite.la mbbackup-0002.mbb
	$(ENGINE_TESTAPP_LAUNCHER) $(ENGINE_TESTAPP) \
                $(ENGINE_TESTS_EXTRA_OPTIONS) \
                -E .libs/ep.so -t $(TEST_TIMEOUT) \
		-T .libs/ep_testsuite.so \
		-e 'flushall_enabled=true;ht_size=13;ht_locks=7;initfile=t/test_pragma.sql;min_data_age=0;db_strategy=multiMTVBDB'

test: all check-TESTS engine_tests sizes
	./sizes

if HAVE_DTRACE
BUILT_SOURCES += dtrace/probes.h
CLEANFILES += dtrace/probes.h

ep_la_SOURCES += dtrace/probes.d

if DTRACE_NEEDS_OBJECTS
ep_la_LIBADD += ep_la-probes.lo
ep_la_DEPENDENCIES +=  ep_la-probes.lo
ep_testsuite_la_LIBADD += ep_testsuite_la-probes.lo
ep_testsuite_la_DEPENDENCIES += ep_testsuite_la-probes.lo
management_cbdbconvert_LDADD += .libs/cddbconvert-probes.o
management_cbdbconvert_DEPENDENCIES += .libs/cddbconvert-probes.o
atomic_ptr_test_LDADD = .libs/atomic_ptr_test-probes.o
atomic_ptr_test_DEPENDENCIES += .libs/atomic_ptr_test-probes.o
atomic_test_LDADD = .libs/atomic_test-probes.o
atomic_test_DEPENDENCIES += .libs/atomic_test-probes.o
checkpoint_test_LDADD += .libs/checkpoint_test-probes.o
checkpoint_test_DEPENDENCIES += .libs/checkpoint_test-probes.o
dispatcher_test_LDADD += .libs/dispatcher_test-probes.o
dispatcher_test_DEPENDENCIES += .libs/dispatcher_test-probes.o
hash_table_test_LDADD += .libs/hash_table_test-probes.o
hash_table_test_DEPENDENCIES += .libs/hash_table_test-probes.o
vbucket_test_LDADD += .libs/vbucket_test-probes.o
vbucket_test_DEPENDENCIES += .libs/vbucket_test-probes.o
mutex_test_LDADD = .libs/mutex_test-probes.o
mutex_test_DEPENDENCIES += .libs/mutex_test-probes.o

CLEANFILES += ep_la-probes.o ep_la-probes.lo                            \
              .libs/cddbconvert-probes.o .libs/cddbconvert-probes.o     \
              .libs/atomic_ptr_test-probes.o                            \
              .libs/checkpoint_test-probes.o                            \
              .libs/mutation_test-probes.o                              \
              .libs/dispatcher_test-probes.o                            \
              .libs/hash_table_test-probes.o                            \
              .libs/vbucket_test-probes.o                               \
              .libs/atomic_test-probes.o                                \
              .libs/mutex_test-probes.o
endif
endif

dtrace/probes.h: dtrace/probes.d
	$(DTRACE) $(DTRACEFLAGS) -h \
                  -s $(top_srcdir)/dtrace/probes.d \
                  -o dtrace/probes.h

EP_ENGINE_OBJS=$(ep_la_OBJECTS:%lo=.libs/%o)
ep_la-probes.lo: $(ep_la_OBJECTS) dtrace/probes.h
	$(DTRACE) $(DTRACEFLAGS) -G \
                  -o .libs/ep_la-probes.o \
                  -s ${srcdir}/dtrace/probes.d \
                  $(EP_ENGINE_OBJS)
	sed -e s,item,probes,g ep_la-item.lo > ep_la-probes.lo

EP_TESTSUITE_OBJS=$(ep_testsuite_la_OBJECTS:%lo=.libs/%o)
ep_testsuite_la-probes.lo: $(ep_testsuite_la_OBJECTS) dtrace/probes.h
	$(DTRACE) $(DTRACEFLAGS) -G \
                  -o .libs/ep_testsuite_la-probes.o \
                  -s ${srcdir}/dtrace/probes.d \
                  $(EP_TESTSUITE_OBJS)
	sed -e s,ep_la-item,ep_testsuite_la-probes,g ep_la-item.lo \
               > ep_testsuite_la-probes.lo

.libs/cddbconvert-probes.o: $(management_cbdbconvert_OBJECTS) dtrace/probes.h
	$(DTRACE) $(DTRACEFLAGS) -G \
                  -o .libs/cddbconvert-probes.o \
                  -s ${srcdir}/dtrace/probes.d \
                  $(management_cbdbconvert_OBJECTS)

.libs/atomic_ptr_test-probes.o: $(atomic_ptr_test_OBJECTS) dtrace/probes.h
	$(DTRACE) $(DTRACEFLAGS) -G \
                  -o .libs/atomic_ptr_test-probes.o \
                  -s ${srcdir}/dtrace/probes.d \
                  $(atomic_ptr_test_OBJECTS)

.libs/atomic_test-probes.o: $(atomic_test_OBJECTS) dtrace/probes.h
	$(DTRACE) $(DTRACEFLAGS) -G \
                  -o .libs/atomic_test-probes.o \
                  -s ${srcdir}/dtrace/probes.d \
                  $(atomic_test_OBJECTS)

.libs/checkpoint_test-probes.o: $(checkpoint_test_OBJECTS) dtrace/probes.h
	$(DTRACE) $(DTRACEFLAGS) -G \
                  -o .libs/checkpoint_test-probes.o \
                  -s ${srcdir}/dtrace/probes.d \
                  $(checkpoint_test_OBJECTS)

.libs/mutation_test-probes.o: $(mutation_test_OBJECTS) dtrace/probes.h
	$(DTRACE) $(DTRACEFLAGS) -G \
                  -o .libs/mutation_test-probes.o \
                  -s ${srcdir}/dtrace/probes.d \
                  $(mutation_test_OBJECTS)

.libs/dispatcher_test-probes.o: $(dispatcher_test_OBJECTS) dtrace/probes.h
	$(DTRACE) $(DTRACEFLAGS) -G \
                  -o .libs/dispatcher_test-probes.o \
                  -s ${srcdir}/dtrace/probes.d \
                  $(dispatcher_test_OBJECTS)

.libs/hash_table_test-probes.o: $(hash_table_test_OBJECTS) dtrace/probes.h
	$(DTRACE) $(DTRACEFLAGS) -G \
                  -o .libs/hash_table_test-probes.o \
                  -s ${srcdir}/dtrace/probes.d \
                  $(hash_table_test_OBJECTS)

.libs/vbucket_test-probes.o: $(vbucket_test_OBJECTS) dtrace/probes.h
	$(DTRACE) $(DTRACEFLAGS) -G \
                  -o .libs/vbucket_test-probes.o \
                  -s ${srcdir}/dtrace/probes.d \
                  $(vbucket_test_OBJECTS)

.libs/mutex_test-probes.o: $(mutex_test_OBJECTS) dtrace/probes.h
	$(DTRACE) $(DTRACEFLAGS) -G \
                  -o .libs/mutex_test-probes.o \
                  -s ${srcdir}/dtrace/probes.d \
                  $(mutex_test_OBJECTS)

reformat:
	astyle --mode=c \
               --quiet \
               --style=1tbs \
               --indent=spaces=4 \
               --indent-namespaces \
               --indent-col1-comments \
               --max-instatement-indent=78 \
               --pad-oper \
               --pad-header \
               --unpad-paren \
               --add-brackets \
               --align-pointer=name \
               --align-reference=name \
               $(top_srcdir)/couch-kvstore/*.[ch][ch]
