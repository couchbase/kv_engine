add_library(mc_program_utils OBJECT
            getpass.cc getpass.h
            hostname_utils.cc hostname_utils.h)
kv_enable_pch(mc_program_utils)
target_link_libraries(mc_program_utils PRIVATE platform ${COUCHBASE_NETWORK_LIBS})

cb_add_test_executable(memcached-hostutils-test
               hostname_utils_test.cc
               $<TARGET_OBJECTS:mc_program_utils>)
kv_enable_pch(memcached-hostutils-test)
target_link_libraries(memcached-hostutils-test PRIVATE
                      cbsocket
                      platform
                      GTest::gtest
                      GTest::gtest_main)
add_test(NAME memcached-hostutils-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached-hostutils-test)

add_subdirectory(dcpdrain)
add_subdirectory(dcplatency)
add_subdirectory(engine_testapp)
add_subdirectory(kvlite)
add_subdirectory(mcbasher)
add_subdirectory(mcthrottlectl)
add_subdirectory(mcctl)
add_subdirectory(mcifconfig)
add_subdirectory(mclogsplit)
add_subdirectory(mcstat)
add_subdirectory(mctestauth)
add_subdirectory(mctimings)
add_subdirectory(mctrace)
add_subdirectory(rangescan)
