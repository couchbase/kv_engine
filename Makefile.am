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
                src/generated_configuration.h \
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
                 src/access_scanner.h \
                 src/atomic/gcc_atomics.h \
                 src/atomic/libatomic.h \
                 src/atomic.cc src/atomic.h \
                 src/backfill.h \
                 src/backfill.cc \
                 src/bgfetcher.h \
                 src/bgfetcher.cc \
                 src/callbacks.h \
                 src/checkpoint.h \
                 src/checkpoint.cc \
                 src/checkpoint_remover.h \
                 src/checkpoint_remover.cc \
                 src/common.h \
                 src/config_static.h \
                 src/dispatcher.cc src/dispatcher.h \
                 src/ep.cc src/ep.h \
                 src/ep_engine.cc src/ep_engine.h \
                 src/ep_time.c src/ep_time.h \
                 src/flusher.cc src/flusher.h \
                 src/histo.h \
                 src/htresizer.cc src/htresizer.h \
                 src/item.cc src/item.h \
                 src/item_pager.cc src/item_pager.h \
                 src/kvstore.h \
                 src/locks.h \
                 src/memory_tracker.cc src/memory_tracker.h \
                 src/mutex.cc src/mutex.h \
                 src/priority.cc src/priority.h \
                 src/queueditem.cc src/queueditem.h \
                 src/ringbuffer.h \
                 src/sizes.cc \
                 src/stats.h \
                 src/stats-info.h src/stats-info.c \
                 src/statsnap.cc src/statsnap.h \
                 src/statwriter.h \
                 src/stored-value.cc src/stored-value.h \
                 src/syncobject.h \
                 src/tapconnection.cc src/tapconnection.h \
                 src/tapconnmap.cc src/tapconnmap.h \
                 src/tapthrottle.cc src/tapthrottle.h \
                 src/vbucket.cc src/vbucket.h \
                 src/vbucketmap.cc src/vbucketmap.h \
                 src/warmup.cc src/warmup.h


libobjectregistry_la_CPPFLAGS = $(AM_CPPFLAGS)
libobjectregistry_la_SOURCES = src/objectregistry.cc src/objectregistry.h

libkvstore_la_SOURCES = src/crc32.c src/crc32.h src/kvstore.cc src/kvstore.h  \
                        src/mutation_log.cc src/mutation_log.h                \
                        src/mutation_log_compactor.cc                         \
                        src/mutation_log_compactor.h
libkvstore_la_CPPFLAGS = $(AM_CPPFLAGS)

libblackhole_kvstore_la_CPPFLAGS = -I$(top_srcdir)/src/blackhole-kvstore \
                                   $(AM_CPPFLAGS)
libblackhole_kvstore_la_SOURCES = src/kvstore.h \
                                  src/blackhole-kvstore/blackhole.cc \
                                  src/blackhole-kvstore/blackhole.h

.generated_stat-info: gen_code docs/stats.json
	./gen_code -j docs/stats.json -h src/stats-info.h -c src/stats-info.c \
        -f get_stats_info && touch .generated_stat-info

src/stats-info.h src/stats-info.c: .generated_stat-info

.generated_configuration: gen_config configuration.json
	./gen_config && touch .generated_configuration

src/generated_configuration.h src/generated_configuration.cc: .generated_configuration

libdirutils_la_SOURCES = src/couch-kvstore/dirutils.cc \
                         src/couch-kvstore/dirutils.h


libcouch_kvstore_la_SOURCES = src/kvstore.h

if HAVE_LIBCOUCHSTORE
libcouch_kvstore_la_SOURCES += src/couch-kvstore/couch-kvstore.cc    \
                               src/couch-kvstore/couch-kvstore.h     \
                               src/couch-kvstore/couch-fs-stats.cc   \
                               src/couch-kvstore/couch-fs-stats.h    \
                               src/couch-kvstore/couch-notifier.cc   \
                               src/couch-kvstore/couch-notifier.h    \
                               tools/cJSON.c                         \
                               tools/cJSON.h                         \
                               tools/JSON_checker.c                  \
                               tools/JSON_checker.h

libcouch_kvstore_la_LIBADD = libdirutils.la $(LTLIBCOUCHSTORE)
else
libcouch_kvstore_la_SOURCES += src/couch-kvstore/couch-kvstore-dummy.h \
                               src/couch-kvstore/couch-kvstore-dummy.cc
endif

libcouch_kvstore_la_CPPFLAGS = -I$(top_srcdir)/src/couch-kvstore $(AM_CPPFLAGS)

libconfiguration_la_SOURCES = src/generated_configuration.h \
                              src/configuration.h \
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
               checkpoint_test \
               chunk_creation_test \
               dispatcher_test \
               hash_table_test \
               histo_test \
               hrtime_test \
               json_test \
               misc_test \
               mutation_log_test \
               mutex_test \
               priority_test \
               ringbuffer_test \
               vbucket_test

if HAVE_GOOGLETEST
check_PROGRAMS += dirutils_test
endif

TESTS=${check_PROGRAMS}
EXTRA_TESTS =

ep_testsuite_la_CPPFLAGS = -I$(top_srcdir)/tests $(AM_CPPFLAGS) ${NO_WERROR}
ep_testsuite_la_SOURCES= tests/ep_testsuite.cc tests/ep_testsuite.h       \
                         src/atomic.cc src/mutex.cc src/mutex.h           \
                         src/item.cc src/testlogger_libify.cc             \
                         src/dispatcher.cc src/ep_time.c src/locks.h      \
                         src/ep_time.h         \
                         tests/mock/mccouch.cc tests/mock/mccouch.h       \
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
atomic_test_SOURCES = tests/module_tests/atomic_test.cc src/atomic.h \
                      src/testlogger.cc src/mutex.cc
atomic_test_DEPENDENCIES = src/atomic.h

atomic_ptr_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
atomic_ptr_test_SOURCES = tests/module_tests/atomic_ptr_test.cc src/atomic.cc \
                          src/atomic.h src/testlogger.cc src/mutex.cc        \
                          src/mutex.h
atomic_ptr_test_DEPENDENCIES = src/atomic.h

mutex_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
mutex_test_SOURCES = tests/module_tests/mutex_test.cc src/locks.h \
                     src/testlogger.cc src/mutex.cc
mutex_test_DEPENDENCIES = src/locks.h

dispatcher_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
dispatcher_test_SOURCES = tests/module_tests/dispatcher_test.cc \
                          src/dispatcher.cc	src/dispatcher.h    \
                          src/priority.cc src/priority.h 	    \
                          src/testlogger.cc src/atomic.cc src/mutex.cc
dispatcher_test_DEPENDENCIES = src/common.h  src/dispatcher.h       \
                               src/dispatcher.cc src/priority.cc 	\
                               src/priority.h libobjectregistry.la
dispatcher_test_LDADD = libobjectregistry.la

hash_table_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
hash_table_test_SOURCES = tests/module_tests/hash_table_test.cc src/item.cc  \
                          src/stored-value.cc src/stored-value.h             \
                          src/testlogger.cc src/atomic.cc src/mutex.cc       \
                          tools/cJSON.c src/memory_tracker.h                 \
                          tests/module_tests/test_memory_tracker.cc
hash_table_test_DEPENDENCIES = src/stored-value.cc src/stored-value.h    \
                               src/ep.h src/item.h libobjectregistry.la
hash_table_test_LDADD = libobjectregistry.la

misc_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
misc_test_SOURCES = tests/module_tests/misc_test.cc src/common.h
misc_test_DEPENDENCIES = src/common.h

json_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
json_test_SOURCES = tests/module_tests/json_test.cc tools/JSON_checker.c \
                    tools/JSON_checker.h src/common.h
json_test_DEPENDENCIES = src/common.h

priority_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
priority_test_SOURCES = tests/module_tests/priority_test.cc src/priority.h \
                        src/priority.cc

sizes_CPPFLAGS = $(AM_CPPFLAGS)
sizes_SOURCES = src/sizes.cc src/mutex.h src/mutex.cc src/testlogger.cc
sizes_DEPENDENCIES = src/vbucket.h src/stored-value.h src/item.h

gen_config_CPPFLAGS = -I$(top_srcdir)/tools $(AM_CPPFLAGS)
gen_config_SOURCES = tools/genconfig.cc tools/cJSON.c tools/cJSON.h
gen_config_LDFLAGS = -lm

gen_code_CPPFLAGS = -I$(top_srcdir)/tools $(AM_CPPFLAGS)
gen_code_SOURCES = tools/gencode.cc tools/cJSON.c tools/cJSON.h

vbucket_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
vbucket_test_SOURCES = tests/module_tests/vbucket_test.cc              \
               tests/module_tests/threadtests.h  src/vbucket.h	       \
               src/vbucket.cc src/stored-value.cc src/stored-value.h   \
               src/atomic.cc src/testlogger.cc src/checkpoint.h 	   \
               src/checkpoint.cc src/byteorder.c src/vbucketmap.cc     \
               src/mutex.cc tests/module_tests/test_memory_tracker.cc  \
               src/memory_tracker.h  src/item.cc tools/cJSON.c         \
               src/bgfetcher.h src/dispatcher.h src/dispatcher.cc
vbucket_test_DEPENDENCIES = src/vbucket.h src/stored-value.cc     \
                            src/stored-value.h src/checkpoint.h  \
                            src/checkpoint.cc libobjectregistry.la \
                            libconfiguration.la
vbucket_test_LDADD = libobjectregistry.la libconfiguration.la

checkpoint_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
checkpoint_test_SOURCES = tests/module_tests/checkpoint_test.cc                \
                          src/checkpoint.h src/checkpoint.cc src/vbucket.h     \
                          src/vbucket.cc src/testlogger.cc src/stored-value.cc \
                          src/stored-value.h src/queueditem.h                  \
                          src/byteorder.c src/atomic.cc src/mutex.cc           \
                          tests/module_tests/test_memory_tracker.cc            \
                          src/memory_tracker.h src/item.cc tools/cJSON.c       \
                          src/bgfetcher.h src/dispatcher.h src/dispatcher.cc
checkpoint_test_DEPENDENCIES = src/checkpoint.h src/vbucket.h           \
              src/stored-value.cc src/stored-value.h  src/queueditem.h  \
              libobjectregistry.la libconfiguration.la
checkpoint_test_LDADD = libobjectregistry.la libconfiguration.la

dirutils_test_SOURCES = tests/module_tests/dirutils_test.cc
dirutils_test_DEPENDENCIES = libdirutils.la
dirutils_test_LDADD = libdirutils.la
dirutils_test_LDFLAGS = -lgtest

mutation_log_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
mutation_log_test_SOURCES = tests/module_tests/mutation_log_test.cc         \
                            src/mutation_log.h	src/testlogger.cc           \
                            src/mutation_log.cc src/byteorder.c src/crc32.h \
                            src/crc32.c src/vbucketmap.cc src/item.cc       \
                            src/atomic.cc src/mutex.cc src/stored-value.cc  \
                            src/ep_time.c src/checkpoint.cc
mutation_log_test_DEPENDENCIES = src/mutation_log.h
mutation_log_test_LDADD = libobjectregistry.la libconfiguration.la

hrtime_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
hrtime_test_SOURCES = tests/module_tests/hrtime_test.cc src/common.h

histo_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
histo_test_SOURCES = tests/module_tests/histo_test.cc src/common.h src/histo.h
histo_test_DEPENDENCIES = src/common.h src/histo.h

chunk_creation_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
chunk_creation_test_SOURCES = tests/module_tests/chunk_creation_test.cc \
                              src/common.h

ringbuffer_test_CXXFLAGS = $(AM_CPPFLAGS) $(AM_CXXFLAGS) ${NO_WERROR}
ringbuffer_test_SOURCES = tests/module_tests/ringbuffer_test.cc src/ringbuffer.h
ringbuffer_test_DEPENDENCIES = src/ringbuffer.h

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

cpplint:
	python tools/cpplint.py --verbose=0 \
        --filter=-,+legal,+build,-build/namespaces \
        src/*.cc src/*.h src/atomic/*.h \
        src/blackhole-kvstore/*.cc src/blackhole-kvstore/*.h \
        src/couch-kvstore/*.cc src/couch-kvstore/*.h \
        tests/*.cc tests/*.h \
        tests/mock/*.cc tests/mock/*.h \
        tests/module_tests/*.cc tests/module_tests/*.h

engine_tests: ep.la ep_testsuite.la
	$(ENGINE_TESTAPP_LAUNCHER) $(ENGINE_TESTAPP) \
                $(ENGINE_TESTS_EXTRA_OPTIONS) \
                -E .libs/ep.so -t $(TEST_TIMEOUT) \
		-T .libs/ep_testsuite.so \
		-e 'flushall_enabled=true;ht_size=13;ht_locks=7;'

test: all check-TESTS engine_tests cpplint sizes
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
