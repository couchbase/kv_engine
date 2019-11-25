add_executable(memcached-basic-engine-tests
               basic_engine_testsuite.cc
               $<TARGET_OBJECTS:engine_testapp>)
add_sanitizers(memcached-basic-engine-tests)
target_link_libraries(memcached-basic-engine-tests
                      engine_testapp)

add_test(NAME memcached-basic-engine-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached-basic-engine-tests -E mc)
