cb_add_test_executable(serverless_test
        main.cc serverless_test.cc serverless_test.h)
target_link_libraries(serverless_test PRIVATE cluster_framework)
add_dependencies(serverless_test memcached)
add_sanitizers(serverless_test)
cb_enable_unity_build(serverless_test)
kv_enable_pch(serverless_test)
add_test(NAME serverless_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND serverless_test)
