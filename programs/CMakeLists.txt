add_library(mc_program_utils OBJECT
        getpass.cc getpass.h
        hostname_utils.cc hostname_utils.h
        parse_tls_option.cc parse_tls_option.h
        mc_program_getopt.cc
        mc_program_getopt.h
        )
kv_enable_pch(mc_program_utils)
target_link_libraries(mc_program_utils PRIVATE platform ${COUCHBASE_NETWORK_LIBS})

cb_add_test_executable(mc_program_utils_test
        hostname_utils_test.cc
        $<TARGET_OBJECTS:mc_program_utils>)
kv_enable_pch(mc_program_utils_test)
target_link_libraries(mc_program_utils_test PRIVATE
                      mc_client_connection
                      mcd_util
                      cbsocket
                      platform
                      GTest::gtest
                      GTest::gtest_main)
add_test(NAME mc_program_utils_test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND mc_program_utils_test)

add_subdirectory(dbfill)
add_subdirectory(dcpdrain)
add_subdirectory(dcplatency)
add_subdirectory(engine_testapp)
add_subdirectory(kvlite)
add_subdirectory(mcauthjammer)
add_subdirectory(mcbasher)
add_subdirectory(mcctl)
add_subdirectory(mcifconfig)
add_subdirectory(mcstat)
add_subdirectory(mctestauth)
add_subdirectory(mctimings)
add_subdirectory(mctrace)
add_subdirectory(rangescan)
