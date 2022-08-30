cb_add_test_executable(serverless_test
        config_bucket_test.cc
        dcp_drain.cc
        dcp_drain.h
        dcp_metering_test.cc
        main.cc
        misc_test.cc
        metering_test.cc
        serverless_pause_resume_test.cc
        serverless_test.h
        throttle_test.cc
        )
target_link_libraries(serverless_test PRIVATE cluster_framework xattr)
add_dependencies(serverless_test memcached)
add_sanitizers(serverless_test)
cb_enable_unity_build(serverless_test)
kv_enable_pch(serverless_test)

if(EP_USE_MAGMA)
    set(SERVERLESS_TEST_BACKEND magma)
else()
    set(SERVERLESS_TEST_BACKEND couchdb)
endif()

add_test(NAME serverless_test_${SERVERLESS_TEST_BACKEND}
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND serverless_test --backend=${SERVERLESS_TEST_BACKEND})
