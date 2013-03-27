# Please list files alphabetically in the lists to reduce the risk for
# a merge conflict.
#
ACLOCAL_AMFLAGS = -I m4 --force
man_MANS = doc/memcached.1
bin_PROGRAMS = engine_testapp memcached mcstat mcbasher isasladm
noinst_PROGRAMS = sizes testapp timedrun bucket_engine_testapp
pkginclude_HEADERS = \
                     include/memcached/allocator_hooks.h \
                     include/memcached/callback.h \
                     include/memcached/config_parser.h \
                     include/memcached/engine.h \
                     include/memcached/engine_common.h \
                     include/memcached/engine_testapp.h \
                     include/memcached/extension.h \
                     include/memcached/extension_loggers.h \
                     include/memcached/protocol_binary.h \
                     include/memcached/protocol_plugin.h \
                     include/memcached/server_api.h \
                     include/memcached/types.h \
                     include/memcached/util.h \
                     include/memcached/vbucket.h \
                     include/memcached/visibility.h

# libmemcached_utilities.la needs to be listed first because some of the
# other modules depend on it
pkglib_LTLIBRARIES = libmemcached_utilities.la \
                     ascii_scrub.la \
                     basic_engine_testsuite.la \
                     bucket_engine.la \
                     default_engine.la \
                     example_protocol.la \
                     file_logger.la \
                     fragment_rw_ops.la \
                     blackhole_logger.la \
                     stdin_term_handler.la \
                     tap_mock_engine.la

if BUILD_SYSLOG_LOGGER
pkglib_LTLIBRARIES += syslog_logger.la
endif

if BUILD_EVENTLOG_LOGGER
pkglib_LTLIBRARIES += eventlog_logger.la
endif

noinst_LTLIBRARIES= libgenhash.la bucket_engine_mock_engine.la

sizes_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/daemon
sizes_SOURCES = programs/sizes.c


# Test application to test stuff from C
testapp_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/daemon
testapp_SOURCES = programs/testapp.c
testapp_DEPENDENCIES= libmemcached_utilities.la
testapp_LDADD= libmemcached_utilities.la $(APPLICATION_LIBS)

mcstat_SOURCES = programs/mcstat.c
mcstat_LDADD = $(APPLICATION_LIBS)

mcbasher_SOURCES = programs/mcbasher.cc
mcbasher_LDADD = $(APPLICATION_LIBS)

# New and fancy test program to test engines without the need to run
# everything through the network layer
engine_testapp_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/programs
engine_testapp_SOURCES = \
                        programs/engine_testapp.c \
                        programs/mock_server.c \
                        programs/mock_server.h \
                        include/memcached/allocator_hooks.h \
                        include/memcached/engine_testapp.h
engine_testapp_DEPENDENCIES= libmemcached_utilities.la
engine_testapp_LDADD= libmemcached_utilities.la $(APPLICATION_LIBS)

# Small application used start another application and terminate it after
# a certain amount of time
timedrun_SOURCES = programs/timedrun.c

# A collection of functions used by the various modules in memcached
libmemcached_utilities_la_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/utilities
libmemcached_utilities_la_LDFLAGS =-R '$(pkglibdir)' -R '$(libdir)'
libmemcached_utilities_la_SOURCES= \
                        include/memcached/config_parser.h \
                        include/memcached/util.h \
                        utilities/config_parser.c \
                        utilities/engine_loader.c \
                        utilities/engine_loader.h \
                        utilities/extension_loggers.c \
                        utilities/util.c

memcached_SOURCES = \
                    daemon/cache.h \
                    config_static.h \
                    daemon/daemon.c \
                    daemon/hash.c \
                    daemon/hash.h \
                    daemon/memcached.c\
                    daemon/memcached.h \
                    daemon/sasl_defs.h \
                    daemon/sasl_defs.c \
                    daemon/isasl.c \
                    daemon/isasl.h \
                    daemon/privileges.c \
                    daemon/stats.c \
                    daemon/stats.h \
                    daemon/thread.c \
                    daemon/alloc_hooks.c \
                    daemon/alloc_hooks.h \
                    trace.h

memcached_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/daemon
memcached_LDFLAGS =-R '$(pkglibdir)' -R '$(libdir)'
memcached_CFLAGS = @PROFILER_FLAGS@
memcached_DEPENDENCIES = libmemcached_utilities.la
memcached_LDADD = @PROFILER_LDFLAGS@ $(MALLOC_LIBS) libmemcached_utilities.la -levent $(APPLICATION_LIBS)

if BUILD_CACHE
memcached_SOURCES += daemon/cache.c
testapp_SOURCES += daemon/cache.c
endif

isasladm_SOURCES = programs/isasladm.c
isasladm_LDADD = $(APPLICATION_LIBS)

CLEANFILES=
BUILT_SOURCES=

# The default storage engine
default_engine_la_SOURCES= engines/default_engine/assoc.c \
                           engines/default_engine/assoc.h \
                           engines/default_engine/default_engine.c \
                           engines/default_engine/default_engine.h \
                           engines/default_engine/items.c \
                           engines/default_engine/items.h \
                           engines/default_engine/slabs.c \
                           engines/default_engine/slabs.h

default_engine_la_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/engines/default_engine
default_engine_la_DEPENDENCIES= libmemcached_utilities.la
default_engine_la_LIBADD= libmemcached_utilities.la $(LIBM)
default_engine_la_LDFLAGS= -avoid-version -shared -module -no-undefined

# A mock engine I may use to test tap
tap_mock_engine_la_SOURCES = engines/tap_mock_engine/tap_mock_engine.cc \
                             engines/tap_mock_engine/tap_mock_engine.h

tap_mock_engine_la_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/engines/tap_mock_engine
tap_mock_engine_la_DEPENDENCIES= libmemcached_utilities.la
tap_mock_engine_la_LIBADD= libmemcached_utilities.la $(LIBM)
tap_mock_engine_la_LDFLAGS= -avoid-version -shared -module -no-undefined

if BUILD_DTRACE
BUILT_SOURCES += memcached_dtrace.h
CLEANFILES += memcached_dtrace.h
endif

if DTRACE_INSTRUMENT_OBJ
memcached_LDADD += memcached_dtrace.o
memcached_DEPENDENCIES += memcached_dtrace.o
CLEANFILES += memcached_dtrace.o engines/default_engine/default_engine_la-dtrace.lo

default_engine_la_LIBADD += engines/default_engine/default_engine_la-dtrace.lo
default_engine_la_DEPENDENCIES += engines/default_engine/default_engine_la-dtrace.lo
endif

bucket_engine_la_SOURCES= engines/bucket_engine/bucket_engine.c \
                          engines/bucket_engine/bucket_engine.h \
                          engines/bucket_engine/topkeys.c \
                          engines/bucket_engine/topkeys.h \
                          engines/bucket_engine/bucket_engine_internal.h
bucket_engine_la_LDFLAGS= -avoid-version -shared -module -no-undefined
bucket_engine_la_LIBADD = libgenhash.la

bucket_engine_mock_engine_la_SOURCES = engines/bucket_engine/mock_engine.c
bucket_engine_mock_engine_la_LDFLAGS = -module -dynamic -rpath /nowhere
bucket_engine_mock_engine_la_LIBADD = libgenhash.la
bucket_engine_mock_engine_la_DEPENDENCIES = libgenhash.la

bucket_engine_testapp_SOURCES = engines/bucket_engine/testapp.c
bucket_engine_testapp_DEPENDENCIES = bucket_engine_mock_engine.la \
                                     libgenhash.la bucket_engine.la
bucket_engine_testapp_LDADD = libgenhash.la $(APPLICATION_LIBS)

libgenhash_la_SOURCES = engines/bucket_engine/genhash.c \
                        engines/bucket_engine/genhash.h \
                        engines/bucket_engine/genhash_int.h

# ASCII support for the scrub command
ascii_scrub_la_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/extensions
ascii_scrub_la_SOURCES= extensions/protocol/ascii_scrub.c \
                        extensions/protocol_extension.h
ascii_scrub_la_LDFLAGS= -avoid-version -shared -module -no-undefined

# An extension that supports partital read/write operation
fragment_rw_ops_la_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/extensions
fragment_rw_ops_la_SOURCES = extensions/protocol/fragment_rw.c \
                             extensions/protocol/fragment_rw.h
fragment_rw_ops_la_LDFLAGS = -avoid-version -shared -module -no-undefined
fragment_rw_ops_la_LIBADD = libmemcached_utilities.la $(LIBM)
fragment_rw_ops_la_DEPENDENCIES = libmemcached_utilities.la

# An extension that will shut down memcached if standard input is closed
stdin_term_handler_la_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/extensions
stdin_term_handler_la_SOURCES= extensions/daemon/stdin_check.c \
                               extensions/protocol_extension.h
stdin_term_handler_la_LDFLAGS= -avoid-version -shared -module -no-undefined

# An example extension that extends the ascii protocol with noop and echo
example_protocol_la_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/extensions
example_protocol_la_SOURCES= extensions/protocol/example_protocol.c \
                             extensions/protocol_extension.h
example_protocol_la_LDFLAGS= -avoid-version -shared -module -no-undefined

# A logger just ignoring the output
blackhole_logger_la_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/extensions
blackhole_logger_la_SOURCES = extensions/loggers/blackhole_logger.c \
                              extensions/protocol_extension.h
blackhole_logger_la_LDFLAGS = -avoid-version -shared -module -no-undefined

# A logger using syslog
syslog_logger_la_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/extensions
syslog_logger_la_SOURCES = extensions/loggers/syslog_logger.c \
                           extensions/protocol_extension.h
syslog_logger_la_LDFLAGS = -avoid-version -shared -module -no-undefined

# A logger using eventlog
eventlog_logger_la_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/extensions
eventlog_logger_la_SOURCES = extensions/loggers/eventlog_logger.c \
                             extensions/protocol_extension.h
eventlog_logger_la_LDFLAGS = -avoid-version -shared -module -no-undefined

# The file-based logger doing compression
file_logger_la_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/extensions
file_logger_la_SOURCES = extensions/loggers/file_logger.c \
                         extensions/protocol_extension.h
file_logger_la_LDFLAGS = -avoid-version -shared -module -no-undefined
file_logger_la_LIBADD = $(LIBZ)

basic_engine_testsuite_la_SOURCES= testsuite/basic_engine_testsuite.c \
                                   testsuite/basic_engine_testsuite.h
basic_engine_testsuite_la_DEPENDENCIES= libmemcached_utilities.la
basic_engine_testsuite_la_LIBADD= libmemcached_utilities.la $(LIBM)
basic_engine_testsuite_la_LDFLAGS= -avoid-version -shared -module -no-undefined

breakdancer_testsuite_la_CPPFLAGS = $(CPPFLAGS) -I$(top_srcdir)/testsuite/breakdancer
breakdancer_testsuite_la_SOURCES= breakdancer_testsuite.c \
                                  testsuite/breakdancer/suite_stubs.c \
                                  testsuite/breakdancer/suite_stubs.h
breakdancer_testsuite_la_LDFLAGS= -avoid-version -shared -module -no-undefined -rpath /nowhere

breakdancer_testsuite.c: testsuite/breakdancer/breakdancer.py testsuite/breakdancer/engine_test.py
	${top_srcdir}/testsuite/breakdancer/engine_test.py > breakdancer_testsuite.c || ( rm breakdancer_testsuite.c && /bin/false)

ENGINE_TESTS=test_engine

if ENABLE_BREAKDANCE
noinst_LTLIBRARIES += breakdancer_testsuite.la
BUILT_SOURCES += breakdancer_testsuite.c
CLEANFILES += breakdancer_testsuite.c
ENGINE_TESTS += breakdancer_engine
endif

memcached_dtrace.h: memcached_dtrace.d
	${DTRACE} -h -s $(top_srcdir)/memcached_dtrace.d
	sed -e 's,void \*,const void \*,g' memcached_dtrace.h | \
            sed -e 's,char \*,const char \*,g' | tr '\t' ' ' > mmc_dtrace.tmp
	mv mmc_dtrace.tmp memcached_dtrace.h

memcached_dtrace.o: $(memcached_OBJECTS)
	$(DTRACE) $(DTRACEFLAGS) -G -o memcached_dtrace.o \
                 -s ${srcdir}/memcached_dtrace.d $(memcached_OBJECTS)

engines/default_engine/default_engine_la-dtrace.lo: $(default_engine_la_OBJECTS)
	${top_srcdir}/config/ltdtrace.sh \
	    $(DTRACE) $(DTRACEFLAGS) -G \
               -o  engines/default_engine/default_engine_la-dtrace.lo \
               -s ${srcdir}/memcached_dtrace.d $(default_engine_la_OBJECTS)

DIST_DIRS = scripts
EXTRA_DIST = doc scripts t memcached_dtrace.d m4/version.m4 \
             testsuite/breakdancer/engine_test.py testsuite/breakdancer/breakdancer.py \
             LICENSE CONTRIBUTING HACKING win32

MOSTLYCLEANFILES = *.gcov *.gcno *.gcda *.tcov

TEST_TIMEOUT=30

breakdancer_engine: engine_testapp breakdancer_testsuite.la
	./engine_testapp -. -q -E .libs/default_engine.so -t $(TEST_TIMEOUT) \
		-T .libs/breakdancer_testsuite.so

test_engine: engine_testapp basic_engine_testsuite.la
	./engine_testapp -E .libs/default_engine.so -t $(TEST_TIMEOUT) \
		-T .libs/basic_engine_testsuite.so

test: $(bin_PROGRAMS) $(noinst_PROGRAMS) $(pkglib_LTLIBRARIES) $(ENGINE_TESTS)
	./sizes
	./testapp
	./bucket_engine_testapp
	prove $(srcdir)/t
	@if test -n "$(PROFILER)"; then \
	  if test `basename $(PROFILER)` = "gcov"; then \
	    for file in memcached-*.gc??; do \
	      mv -f $$file `echo $$file | sed 's/memcached-//'`; \
	    done && \
	    for file in *.gcda; do \
	      srcfile=`echo $$file | sed 's/.gcda/.c/'`; \
	      if test -n "`echo $(memcached_SOURCES) | grep $$srcfile`"; then \
	        echo `$(PROFILER) $$srcfile` | sed 's/'$$srcfile':.*//'; \
	      fi \
	    done \
	  elif test `basename $(PROFILER)` = "tcov"; then \
	    files=`grep SRCFILE memcached.profile/tcovd | sed 's/SRCFILE://' | sort | uniq` && \
	    $(PROFILER) -x memcached.profile $$files 2>&1; \
	    for file in *.tcov; do \
	      srcfile=`echo $$file | sed 's/.tcov//'`; \
	      if test -n "`echo $(memcached_SOURCES) | grep $$srcfile`"; then \
	        echo $$srcfile : `grep 'Percent of the file executed' $$file`; \
	      fi \
	    done \
	  else :; fi \
	else :; fi
