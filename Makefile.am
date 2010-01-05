ACLOCAL_AMFLAGS = -I m4 --force

lib_LTLIBRARIES = ep.la

ep_la_LDFLAGS = -module -dynamic ${LIBSQLITE3}
ep_la_SOURCES = \
                 callbacks.hh \
                 ep.cc ep.hh \
                 ep_engine.cc ep_engine.h \
                 kvstore.hh \
                 locks.hh \
                 sqlite-kvstore.cc sqlite-kvstore.hh
