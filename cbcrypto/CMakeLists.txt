add_library(cbcrypto STATIC
            cbcrypto.cc
            ${PROJECT_SOURCE_DIR}/include/cbcrypto/cbcrypto.h)
kv_enable_pch(cbcrypto)

if (APPLE)
    # Apple is using the Common Crypto Framework for all crypto functions
    target_link_libraries(cbcrypto PRIVATE platform phosphor)
elseif (WIN32)
    # Use the OpenSSL ones for all other platforms
    target_link_libraries(cbcrypto PRIVATE platform phosphor Bcrypt)
else()
    # Use the OpenSSL ones for all other platforms
    target_link_libraries(cbcrypto PRIVATE platform phosphor ${OPENSSL_LIBRARIES})
endif ()

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
