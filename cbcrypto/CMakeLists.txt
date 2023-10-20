add_library(cbcrypto STATIC
            cbcrypto.cc
            ${PROJECT_SOURCE_DIR}/include/cbcrypto/cbcrypto.h)
kv_enable_pch(cbcrypto)
target_link_libraries(cbcrypto PRIVATE platform phosphor OpenSSL::SSL)

cb_add_test_executable(cbcrypto_test
               cbcrypto_test.cc
               ${Memcached_SOURCE_DIR}/include/cbcrypto/cbcrypto.h)
kv_enable_pch(cbcrypto_test)

target_link_libraries(cbcrypto_test PRIVATE
                      cbcrypto
                      platform
                      GTest::gtest GTest::gtest_main)

add_test(NAME cbcrypto-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbcrypto_test)
