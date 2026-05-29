add_compile_options_disable_optimization()

# There is currently and issue with Clang-19 and the fuzz tests
# and it seems to be related to static initialization of variables
# and the order they are initialized. When building with UNITY
# enabled things work as expected, but when building without UNITY
# enabled we see a crash in the fuzz tests related to static
# initialization of variables. The crash is in
# std::char_traits<char>::assign and it seems to be related
# to the fact that the variable being assigned to is not properly
# initialized. When I changed the order of the files being compile
# the crash went away (and that's the solution for now to allow
# us to upgrade the compilers and we can look into the exact
# readon later). The crash looks like this:

# #0  0x00000000005484c0 in std::char_traits<char>::assign (__c1=@0x7fffffffc8c8: 120 'x',
# __c2=<error reading variable: Cannot access memory at address 0x0>) at /opt/gcc-15.2.0/lib/gcc/x86_64-pc-linux-gnu/15.2.0/../../../../include/c++/15.2.0/bits/char_traits.h:352
# #1  0x0000000000548488 in std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> >::_S_copy (
# __d=0x7fffffffc8c8 "x\346\242\365\377\177", __s=0x0, __n=1) at /opt/gcc-15.2.0/lib/gcc/x86_64-pc-linux-gnu/15.2.0/../../../../include/c++/15.2.0/bits/basic_string.h:451
# #2  0x000000000057bbf3 in std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> >::_M_construct<true> (this=0x7fffffffc8b8,
# __str=0x0, __n=0) at /opt/gcc-15.2.0/lib/gcc/x86_64-pc-linux-gnu/15.2.0/../../../../include/c++/15.2.0/bits/basic_string.tcc:298
# #3  0x000000000057bb27 in std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> >::basic_string (this=0x7fffffffc8b8, __str=...)
# at /opt/gcc-15.2.0/lib/gcc/x86_64-pc-linux-gnu/15.2.0/../../../../include/c++/15.2.0/bits/basic_string.h:617
# #4  0x0000000000951251 in CollectionEntry::Entry::Entry (this=0x7fffffffc8b8) at /home/trond/compile/trunk/kv_engine/utilities/test_manifest.h:65
# #5  0x0000000001f10ce4 in cb::fuzzing::collectionEntry () at /home/trond/compile/trunk/kv_engine/engines/ep/tests/fuzz_tests/fuzz_test_helpers.cc:193
# #6  0x0000000001e9ae00 in cb::fuzzing::docKey (defaultCollectionOnly=false)
# at /home/trond/compile/trunk/kv_engine/engines/ep/tests/fuzz_tests/fuzz_test_helpers.h:155
# #7  0x0000000001e97aee in cb::fuzzing::checkpointAction () at /home/trond/compile/trunk/kv_engine/engines/ep/tests/fuzz_tests/fuzz_test_helpers.h:161
# #8  0x0000000001e50fc3 in backfillAndStreamDomains () at /home/trond/compile/trunk/kv_engine/engines/ep/tests/fuzz_tests/active_stream_fuzz_test.cc:221
# #9  0x00000000004f27b0 in __cxx_global_var_init.58(void) ()
# at /home/trond/compile/trunk/kv_engine/engines/ep/tests/fuzz_tests/active_stream_fuzz_test.cc:236
# #10 0x00000000004f2ecb in _GLOBAL__sub_I_active_stream_fuzz_test.cc () at /home/trond/compile/trunk/kv_engine/utilities/test_manifest.h:143
# #11 0x0000000002dc01b5 in __libc_csu_init ()
# #12 0x00007ffff5a4f05a in __libc_start_main () from /usr/lib/libc.so.6
# #13 0x000000000053d42a in _start ()

if (CB_UNITY_BUILD)
    target_sources(ep-engine_ep_unit_tests PUBLIC
            active_stream_fuzz_test.cc
            passive_stream_fuzz_test.cc
            dockey_fuzz_test.cc
            fuzz_test_helpers.cc
    )
endif()

