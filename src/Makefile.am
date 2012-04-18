# Used to build Makefile.in

EXTRA_DIST = ha_pinba.h pinba.h pinba_types.h pinba.pb-c.h threadpool.h protobuf-c-private.h protobuf-c.h

INCLUDES =  $(MYSQL_INC) $(DEPS_CFLAGS)

noinst_HEADERS = ha_pinba.h pinba.h pinba_types.h pinba.pb-c.h threadpool.h protobuf-c-private.h protobuf-c.h

lib_LTLIBRARIES = libpinba_engine.la
libpinba_engine_la_SOURCES = pinba.pb-c.c ha_pinba.cc data.cc tags.cc pool.cc main.cc threadpool.cc protobuf-c.c
libpinba_engine_la_LIBADD = $(DEPS_LIBS)
libpinba_engine_la_LDFLAGS =	-module
