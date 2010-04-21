noinst_PROGRAMS = hash_table_test

hash_table_test_CXXFLAGS = $(AM_CXXFLAGS) -I $(top_srcdir) -Wno-unused-parameter
hash_table_test_SOURCES = hash_table_test.cc ../item.cc
hash_table_test_DEPENDENCIES = ../ep.hh ../item.hh

test: hash_table_test
	./hash_table_test
