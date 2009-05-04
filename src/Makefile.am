# Used to build Makefile.in

EXTRA_DIST = ha_pinba.h pinba.h pinba_types.h pinba-pb.h

INCLUDES =  $(MYSQL_INC) $(DEPS_CFLAGS)

noinst_HEADERS = ha_pinba.h pinba.h pinba_types.h pinba-pb.h

lib_LTLIBRARIES = libpinba_engine.la
libpinba_engine_la_SOURCES = pinba-pb.cc ha_pinba.cc data.cc tags.cc pool.cc main.cc
libpinba_engine_la_LIBADD = $(DEPS_LIBS)
libpinba_engine_la_LDFLAGS =	-module
