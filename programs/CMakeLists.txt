add_library(mc_program_utils OBJECT
        hostname_utils.cc hostname_utils.h
        parse_tls_option.cc parse_tls_option.h
        mc_program_getopt.cc
        mc_program_getopt.h
        )
kv_enable_pch(mc_program_utils)
target_link_libraries(mc_program_utils
        PRIVATE platform ${COUCHBASE_NETWORK_LIBS} Folly::folly
        PUBLIC Folly::headers)

cb_add_test_executable(mc_program_utils_test
        hostname_utils_test.cc
        natsort_test.cc)
kv_enable_pch(mc_program_utils_test)
target_link_libraries(mc_program_utils_test PRIVATE
                      mc_client_connection
                      mc_program_utils
                      mcd_util
                      cbsocket
                      platform
                      GTest::gtest
                      GTest::gtest_main)
add_test(NAME mc_program_utils_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND mc_program_utils_test)

# MB-67520: Enable warnings for narrowing conversions and
# implicit int conversions when building with MSVC and Clang/AppleClang.
# This is limited to a subset of targets whilst we make progress towards
# being able to globally enable such flags.
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
target_compile_options(mc_program_utils_test PRIVATE -Werror=shorten-64-to-32 -Werror=implicit-int-conversion)
endif()

add_subdirectory(dbfill)
add_subdirectory(dcpdrain)
add_subdirectory(dcplatency)
add_subdirectory(engine_testapp)
add_subdirectory(kvlite)
add_subdirectory(mcauthjammer)
add_subdirectory(mcbasher)
add_subdirectory(mcctl)
add_subdirectory(mchistogram)
add_subdirectory(mcifconfig)
add_subdirectory(mclogfmt)
add_subdirectory(mcstat)
add_subdirectory(mctestauth)
add_subdirectory(mctimings)
add_subdirectory(mctopkeys)
add_subdirectory(mctrace)
add_subdirectory(mktoken)
add_subdirectory(rangescan)
