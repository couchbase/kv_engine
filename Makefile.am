ACLOCAL_AMFLAGS = -I m4 --force

lib_LTLIBRARIES = ep.la
noinst_LTLIBRARIES = libsqlite3.la

ep_la_LDFLAGS = -module -dynamic
ep_la_SOURCES = \
                 callbacks.hh \
                 ep.cc ep.hh \
                 ep_engine.cc ep_engine.h \
                 stored-value.hh flusher.hh \
                 kvstore.hh \
                 item.cc item.hh \
                 locks.hh \
                 mutex.hh \
                 sqlite-kvstore.cc sqlite-kvstore.hh \
                 syncobject.hh
ep_la_LIBADD = libsqlite3.la
ep_la_DEPENDENCIES = libsqlite3.la

libsqlite3_la_SOURCES = sqlite3.h sqlite3.c
libsqlite3_la_CFLAGS = $(AM_CFLAGS) ${NO_WERROR}

test:
	cd t && $(MAKE) test
