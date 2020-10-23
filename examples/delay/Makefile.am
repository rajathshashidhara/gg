AM_CPPFLAGS = -I$(srcdir)/../../src/. -I$(builddir)/../../src/. $(CXX14_FLAGS) \
              $(PROTOBUF_CFLAGS) $(CRYPTO_CFLAGS)

AM_CXXFLAGS = $(PICKY_CXXFLAGS) $(EXTRA_CXXFLAGS)

BASE_LDADD = ../../src/thunk/libthunk.a \
             ../../src/sandbox/libggsandbox.a \
             ../../src/trace/libggtrace.a \
             ../../src/trace/libggsyscalltable.a \
             ../../src/protobufs/libggprotobufs.a \
             ../../src/util/libggutil.a \
             $(CRYPTO_LIBS) $(PROTOBUF_LIBS)

bin_PROGRAMS = delay

delay_SOURCES = delay.cc
delay_LDADD = $(BASE_LDADD)
delay_LDFLAGS = -s

if BUILD_STATIC_BINS
  delay_LDFLAGS += -static
endif
