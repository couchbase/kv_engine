add_library(mc_program_utils OBJECT
            getpass.cc getpass.h
            hostname_utils.cc hostname_utils.h)
target_link_libraries(mc_program_utils platform ${COUCHBASE_NETWORK_LIBS})

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    add_executable(memcached-hostutils-test
                   hostname_utils_test.cc
                   $<TARGET_OBJECTS:mc_program_utils>)
    target_link_libraries(memcached-hostutils-test
                          platform
                          gtest
                          gtest_main)
    add_test(NAME memcached-hostutils-test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND memcached-hostutils-test)

    add_subdirectory(engine_testapp)
endif (COUCHBASE_KV_BUILD_UNIT_TESTS)

add_subdirectory(dcpdrain)
add_subdirectory(dcplatency)
add_subdirectory(kvlite)
add_subdirectory(mcctl)
add_subdirectory(mclogsplit)
add_subdirectory(mcstat)
add_subdirectory(mctestauth)
add_subdirectory(mctimings)
add_subdirectory(mctrace)
