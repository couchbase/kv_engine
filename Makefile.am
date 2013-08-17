ACLOCAL_AMFLAGS = -I m4 --force
AM_CPPFLAGS += -I$(top_srcdir)/src -I${top_srcdir}/include

bin_SCRIPTS = management/cbanalyze-core \
              wrapper/cbadm-tap-registration \
              wrapper/cbepctl \
              wrapper/cbstats \
              wrapper/cbvbucketctl

pkginclude_HEADERS = include/ep-engine/command_ids.h


memcachedlibdir = $(libdir)/memcached
memcachedlib_LTLIBRARIES = ep.la ep_testsuite.la timing_tests.la
noinst_LTLIBRARIES = \
                     libblackhole-kvstore.la \
                     libconfiguration.la \
                     libkvstore.la \
                     libobjectregistry.la \
                     libcouch-kvstore.la \
                     libdirutils.la

fixup_stats_json:
	cat docs/stats.json | python -mjson.tool > /tmp/stats.json.tmp
	mv /tmp/stats.json.tmp docs/stats.json

dist_doc_DATA = docs/stats.json

BUILT_SOURCES = src/generated_configuration.cc \
                src/generated_configuration.hh \
                src/stats-info.c src/stats-info.h
CLEANFILES =

EXTRA_DIST = Doxyfile LICENSE README.markdown configuration.json docs \
             dtrace management win32

noinst_PROGRAMS = sizes gen_config gen_code

man_MANS =

ep_la_CPPFLAGS = $(AM_CPPFLAGS)
ep_la_LDFLAGS = -module -dynamic -avoid-version
ep_la_SOURCES =  include/ep-engine/command_ids.h \
                 src/access_scanner.cc \
                 src/access_scanner.hh \
                 src/atomic/gcc_atomics.h \
                 src/atomic/libatomic.h \
                 src/atomic.cc src/atomic.hh \
                 src/backfill.hh \
                 src/backfill.cc \
                 src/bgfetcher.hh \
                 src/bgfetcher.cc \
                 src/callbacks.hh \
                 src/checkpoint.hh \
                 src/checkpoint.cc \
                 src/checkpoint_remover.hh \
                 src/checkpoint_remover.cc \
                 src/common.hh \
                 src/conflict_resolution.cc src/conflict_resolution.h \
                 src/config_static.h \
                 src/dispatcher.cc src/dispatcher.hh \
                 src/ep.cc src/ep.hh \
                 src/ep_engine.cc src/ep_engine.h \
                 src/ep_extension.cc src/ep_extension.h \
                 src/ep_time.c src/ep_time.h \
                 src/flusher.cc src/flusher.hh \
                 src/histo.hh \
                 src/htresizer.cc src/htresizer.hh \
                 src/iomanager/iomanager.cc src/iomanager/iomanager.h \
                 src/item.cc src/item.hh \
                 src/item_pager.cc src/item_pager.hh \
                 src/kvstore.hh \
                 src/kvshard.cc src/kvshard.hh \
                 src/locks.hh \
                 src/memory_tracker.cc src/memory_tracker.hh \
                 src/mutex.cc src/mutex.hh \
                 src/priority.cc src/priority.hh \
                 src/queueditem.cc src/queueditem.hh \
                 src/ringbuffer.hh \
                 src/scheduler.cc src/scheduler.h \
                 src/sizes.cc \
                 src/stats.hh \
                 src/stats-info.h src/stats-info.c \
                 src/statwriter.hh \
                 src/stored-value.cc src/stored-value.hh \
                 src/syncobject.hh \
                 src/tapconnection.cc src/tapconnection.hh \
                 src/tapconnmap.cc src/tapconnmap.hh \
                 src/tapthrottle.cc src/tapthrottle.hh \
                 src/tasks.cc src/tasks.h \
                 src/vbucket.cc src/vbucket.hh \
                 src/vbucketmap.cc src/vbucketmap.hh \
                 src/warmup.cc src/warmup.hh \
                 src/workload.cc src/workload.h

libobjectregistry_la_CPPFLAGS = $(AM_CPPFLAGS)
libobjectregistry_la_SOURCES = src/objectregistry.cc src/objectregistry.hh

libkvstore_la_SOURCES = src/crc32.c src/crc32.h src/kvstore.cc src/kvstore.hh \
                        src/mutation_log.cc src/mutation_log.hh
libkvstore_la_CPPFLAGS = $(AM_CPPFLAGS)

libblackhole_kvstore_la_CPPFLAGS = -I$(top_srcdir)/src/blackhole-kvstore \
                                   $(AM_CPPFLAGS)
libblackhole_kvstore_la_SOURCES = src/kvstore.hh \
                                  src/blackhole-kvstore/blackhole.cc \
                                  src/blackhole-kvstore/blackhole.hh

.generated_stat-info: gen_code docs/stats.json
	./gen_code -j docs/stats.json -h src/stats-info.h -c src/stats-info.c \
        -f get_stats_info && touch .generated_stat-info

src/stats-info.h src/stats-info.c: .generated_stat-info

.generated_configuration: gen_config configuration.json
	./gen_config && touch .generated_configuration

src/generated_configuration.hh src/generated_configuration.cc: .generated_configuration

libdirutils_la_SOURCES = src/couch-kvstore/dirutils.cc \
                         src/couch-kvstore/dirutils.hh


libcouch_kvstore_la_SOURCES = src/kvstore.hh

if HAVE_LIBCOUCHSTORE
libcouch_kvstore_la_SOURCES += src/couch-kvstore/couch-kvstore.cc    \
                               src/couch-kvstore/couch-kvstore.hh    \
                               src/couch-kvstore/couch-fs-stats.cc   \
                               src/couch-kvstore/couch-fs-stats.hh   \
                               src/couch-kvstore/couch-notifier.cc   \
                               src/couch-kvstore/couch-notifier.hh   \
                               tools/cJSON.c                         \
                               tools/cJSON.h                         \
                               tools/JSON_checker.c                  \
                               tools/JSON_checker.h

libcouch_kvstore_la_LIBADD = libdirutils.la $(LTLIBCOUCHSTORE)
else
libcouch_kvstore_la_SOURCES += src/couch-kvstore/couch-kvstore-dummy.hh \
                               src/couch-kvstore/couch-kvstore-dummy.cc
endif

libcouch_kvstore_la_CPPFLAGS = -I$(top_srcdir)/src/couch-kvstore $(AM_CPPFLAGS)

libconfiguration_la_SOURCES = src/generated_configuration.hh \
                              src/configuration.hh \
                              src/configuration.cc

ep_la_LIBADD = libkvstore.la \
               libblackhole-kvstore.la libcouch-kvstore.la \
               libobjectregistry.la libconfiguration.la $(LTLIBEVENT)
ep_la_DEPENDENCIES = libkvstore.la \
               libblackhole-kvstore.la	\
               libobjectregistry.la libconfiguration.la \
               libcouch-kvstore.la
ep_testsuite_la_LIBADD =libobjectregistry.la $(LTLIBEVENT)
ep_testsuite_la_DEPENDENCIES = libobjectregistry.la

check_PROGRAMS=\
               atomic_ptr_test \
               atomic_test \
               chunk_creation_test \
               dispatcher_test \
               hash_table_test \
               histo_test \
               hrtime_test \
               json_test \
               misc_test \
               mutex_test \
               priority_test \
               ringbuffer_test

if HAVE_GOOGLETEST
check_PROGRAMS += dirutils_test
endif

TESTS=${check_PROGRAMS}
EXTRA_TESTS =

ep_testsuite_la_CPPFLAGS = -I$(top_srcdir)/tests $(AM_CPPFLAGS) ${NO_WERROR}
ep_testsuite_la_SOURCES= tests/ep_testsuite.cc tests/ep_testsuite.h       \
                         src/atomic.cc src/mutex.cc src/mutex.hh          \
                         src/item.cc src/testlogger_libify.cc             \
                         src/dispatcher.cc src/ep_time.c src/locks.hh     \
                         src/ep_time.h         \
                         tests/mock/mccouch.cc tests/mock/mccouch.hh      \
                         tests/ep_test_apis.cc tests/ep_test_apis.h
ep_testsuite_la_LDFLAGS= -module -dynamic -avoid-version

# This is because automake can't figure out how to build the same code
# for two different targets.
src/testlogger_libify.cc: src/testlogger.cc
	cp $(top_srcdir)/src/testlogger.cc src/testlogger_libify.cc

if BUILD_GENERATED_TESTS
memcachedlib_LTLIBRARIES += generated_suite.la

.genstamp: tests/module_tests/gen_engine_test.py tests/module_tests/breakdancer.py
	${top_srcdir}/tests/module_tests/gen_engine_test.py
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
generated_suite_la_SOURCES= src/suite_stubs.c tests/ep_testsuite.h
generated_suite_la_LDFLAGS= -module -dynamic -avoid-version

EXTRA_TESTS += generated_engine_tests

GENERATED_TEST_TIMEOUT=5

generated_engine_tests: generated_suite.la
	$(ENGINE_TESTAPP) -E .libs/ep.so -t $(GENERATED_TEST_TIMEOUT) \
		-. -q -T .libs/generated_suite.so \
		-e 'flushall_enabled=true;ht_size=13;ht_locks=7;backend=blackhole'
endif

timing_tests_la_CFLAGS = $(AM_CFLAGS) ${NO_WERROR}
timing_tests_la_SOURCES= tests/module_tests/timing_tests.cc
timing_tests_la_LDFLAGS= -module -dynamic -avoid-version

atomic_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
atomic_test_SOURCES = tests/module_tests/atomic_test.cc src/atomic.hh \
                      src/testlogger.cc src/mutex.cc
atomic_test_DEPENDENCIES = src/atomic.hh

atomic_ptr_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
atomic_ptr_test_SOURCES = tests/module_tests/atomic_ptr_test.cc src/atomic.cc \
                          src/atomic.hh src/testlogger.cc src/mutex.cc        \
                          src/mutex.hh
atomic_ptr_test_DEPENDENCIES = src/atomic.hh

mutex_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
mutex_test_SOURCES = tests/module_tests/mutex_test.cc src/locks.hh \
                     src/testlogger.cc src/mutex.cc
mutex_test_DEPENDENCIES = src/locks.hh

dispatcher_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
dispatcher_test_SOURCES = tests/module_tests/dispatcher_test.cc \
                          src/dispatcher.cc	src/dispatcher.hh   \
                          src/priority.cc src/priority.hh	    \
                          src/testlogger.cc src/atomic.cc src/mutex.cc
dispatcher_test_DEPENDENCIES = src/common.hh src/dispatcher.hh      \
                               src/dispatcher.cc src/priority.cc 	\
                               src/priority.hh libobjectregistry.la
dispatcher_test_LDADD = libobjectregistry.la

hash_table_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
hash_table_test_SOURCES = tests/module_tests/hash_table_test.cc src/item.cc  \
                          src/stored-value.cc src/stored-value.hh            \
                          src/testlogger.cc src/atomic.cc src/mutex.cc       \
                          tools/cJSON.c src/memory_tracker.hh                \
                          tests/module_tests/test_memory_tracker.cc
hash_table_test_DEPENDENCIES = src/stored-value.cc src/stored-value.hh    \
                               src/ep.hh src/item.hh libobjectregistry.la
hash_table_test_LDADD = libobjectregistry.la

misc_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
misc_test_SOURCES = tests/module_tests/misc_test.cc src/common.hh
misc_test_DEPENDENCIES = src/common.hh

json_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
json_test_SOURCES = tests/module_tests/json_test.cc tools/JSON_checker.c \
                    tools/JSON_checker.h src/common.hh
json_test_DEPENDENCIES = src/common.hh

priority_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
priority_test_SOURCES = tests/module_tests/priority_test.cc src/priority.hh \
                        src/priority.cc

sizes_CPPFLAGS = $(AM_CPPFLAGS)
sizes_SOURCES = src/sizes.cc src/mutex.hh src/mutex.cc src/testlogger.cc
sizes_DEPENDENCIES = src/vbucket.hh src/stored-value.hh src/item.hh

gen_config_CPPFLAGS = -I$(top_srcdir)/tools $(AM_CPPFLAGS)
gen_config_SOURCES = tools/genconfig.cc tools/cJSON.c tools/cJSON.h
gen_config_LDFLAGS = -lm

gen_code_CPPFLAGS = -I$(top_srcdir)/tools $(AM_CPPFLAGS)
gen_code_SOURCES = tools/gencode.cc tools/cJSON.c tools/cJSON.h

vbucket_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
vbucket_test_SOURCES = tests/module_tests/vbucket_test.cc              \
               tests/module_tests/threadtests.hh src/vbucket.hh	       \
               src/vbucket.cc src/stored-value.cc src/stored-value.hh  \
               src/atomic.cc src/testlogger.cc src/checkpoint.hh 	   \
               src/checkpoint.cc src/byteorder.c src/vbucketmap.cc     \
               src/mutex.cc tests/module_tests/test_memory_tracker.cc  \
               src/memory_tracker.hh src/item.cc tools/cJSON.c         \
               src/bgfetcher.hh src/dispatcher.hh src/dispatcher.cc    \
               src/kvshard.hh src/kvshard.cc
vbucket_test_DEPENDENCIES = src/vbucket.hh src/stored-value.cc     \
                            src/stored-value.hh src/checkpoint.hh  \
                            src/checkpoint.cc libobjectregistry.la \
                            libconfiguration.la
vbucket_test_LDADD = libobjectregistry.la libconfiguration.la

checkpoint_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
checkpoint_test_SOURCES = tests/module_tests/checkpoint_test.cc                \
                          src/checkpoint.hh src/checkpoint.cc src/vbucket.hh   \
                          src/vbucket.cc src/testlogger.cc src/stored-value.cc \
                          src/stored-value.hh src/queueditem.hh                \
                          src/byteorder.c src/atomic.cc src/mutex.cc           \
                          tests/module_tests/test_memory_tracker.cc            \
                          src/memory_tracker.hh src/item.cc tools/cJSON.c      \
                          src/bgfetcher.hh src/dispatcher.hh src/dispatcher.cc
checkpoint_test_DEPENDENCIES = src/checkpoint.hh src/vbucket.hh         \
              src/stored-value.cc src/stored-value.hh src/queueditem.hh \
              libobjectregistry.la libconfiguration.la
checkpoint_test_LDADD = libobjectregistry.la libconfiguration.la

dirutils_test_SOURCES = tests/module_tests/dirutils_test.cc
dirutils_test_DEPENDENCIES = libdirutils.la
dirutils_test_LDADD = libdirutils.la
dirutils_test_LDFLAGS = -lgtest

mutation_log_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
mutation_log_test_SOURCES = tests/module_tests/mutation_log_test.cc         \
                            src/mutation_log.hh	src/testlogger.cc           \
                            src/mutation_log.cc src/byteorder.c src/crc32.h \
                            src/crc32.c src/vbucketmap.cc src/item.cc       \
                            src/atomic.cc src/mutex.cc src/stored-value.cc  \
                            src/ep_time.c src/checkpoint.cc src/kvshard.hh   \
                            src/kvshard.cc src/kvstore.hh
mutation_log_test_DEPENDENCIES = src/mutation_log.hh
mutation_log_test_LDADD = libobjectregistry.la libconfiguration.la

hrtime_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
hrtime_test_SOURCES = tests/module_tests/hrtime_test.cc src/common.hh

histo_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
histo_test_SOURCES = tests/module_tests/histo_test.cc src/common.hh src/histo.hh
histo_test_DEPENDENCIES = src/common.hh src/histo.hh

chunk_creation_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
chunk_creation_test_SOURCES = tests/module_tests/chunk_creation_test.cc \
                              src/common.hh

ringbuffer_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
ringbuffer_test_SOURCES = tests/module_tests/ringbuffer_test.cc src/ringbuffer.hh
ringbuffer_test_DEPENDENCIES = src/ringbuffer.hh

if BUILD_GETHRTIME
ep_la_SOURCES += src/gethrtime.c
hrtime_test_SOURCES += src/gethrtime.c
dispatcher_test_SOURCES += src/gethrtime.c
vbucket_test_SOURCES += src/gethrtime.c
checkpoint_test_SOURCES += src/gethrtime.c
ep_testsuite_la_SOURCES += src/gethrtime.c
hash_table_test_SOURCES += src/gethrtime.c
mutation_log_test_SOURCES += src/gethrtime.c
endif

if BUILD_BYTEORDER
ep_la_SOURCES += src/byteorder.c
ep_testsuite_la_SOURCES += src/byteorder.c
endif

pythonlibdir=$(libdir)/python
pythonlib_DATA= \
                management/clitool.py \
                management/mc_bin_client.py \
                management/mc_bin_server.py \
                management/memcacheConstants.py \
                management/tap.py \
                management/tap_example.py

pythonlib_SCRIPTS= \
                management/cbadm-tap-registration \
                management/cbepctl \
                management/cbstats \
                management/cbvbucketctl

PYTHON_TOOLS= \
              wrapper/cbadm-tap-registration \
              wrapper/cbepctl \
              wrapper/cbstats \
              wrapper/cbvbucketctl

${PYTHON_TOOLS}: wrapper/wrapper
	cp $< $@

BUILT_SOURCES += ${PYTHON_TOOLS}
CLEANFILES+= ${PYTHON_TOOLS}

TEST_TIMEOUT=60

VALGRIND=valgrind
VALGRIND_OPTIONS=--log-file=valgrind.log --tool=Memcheck --num-callers=50 \
                 --error-limit=yes --track-origins=yes --malloc-fill=55 \
                 --free-fill=aa --dsymutil=yes -q

valgrind_engine_tests: ep.la ep_testsuite.la
	@$(MAKE) \
             ENGINE_TESTAPP_LAUNCHER="$(VALGRIND) $(VALGRIND_OPTIONS)" \
             engine_tests

engine_tests: ep.la ep_testsuite.la
	$(ENGINE_TESTAPP_LAUNCHER) $(ENGINE_TESTAPP) \
                $(ENGINE_TESTS_EXTRA_OPTIONS) \
                -E .libs/ep.so -t $(TEST_TIMEOUT) \
		${IS_LOOP} -T .libs/ep_testsuite.so \
		-e 'flushall_enabled=true;ht_size=13;ht_locks=7;'

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
