ACLOCAL_AMFLAGS = -I m4 --force

lib_LTLIBRARIES = ep.la

ep_la_LDFLAGS = -module -dynamic ${LIBSQLITE3}
ep_la_SOURCES = \
                 ep.cc ep.hh \
                 ep_engine.cc ep_engine.h \
                 sqlite-base.cc sqlite-base.hh