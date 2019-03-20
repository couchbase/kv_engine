if (HAVE_PKCS5_PBKDF2_HMAC_SHA1)
   add_definitions(-DHAVE_PKCS5_PBKDF2_HMAC_SHA1=1)
endif()

if (HAVE_PKCS5_PBKDF2_HMAC)
   add_definitions(-DHAVE_PKCS5_PBKDF2_HMAC=1)
endif()

ADD_LIBRARY(cbcrypto SHARED
            cbcrypto.cc
            ${PROJECT_SOURCE_DIR}/include/cbcrypto/cbcrypto.h)
SET_TARGET_PROPERTIES(cbcrypto PROPERTIES SOVERSION 2.0.0)

if (APPLE)
    # Apple is using the Common Crypto Framework for all crypto functions
    TARGET_LINK_LIBRARIES(cbcrypto PRIVATE platform phosphor)
elseif (WIN32)
    # Use the OpenSSL ones for all other platforms
    TARGET_LINK_LIBRARIES(cbcrypto PRIVATE platform phosphor Bcrypt)
else()
    # Use the OpenSSL ones for all other platforms
    TARGET_LINK_LIBRARIES(cbcrypto PRIVATE platform phosphor ${OPENSSL_LIBRARIES})
endif ()

GENERATE_EXPORT_HEADER(cbcrypto
                       EXPORT_MACRO_NAME CBCRYPTO_PUBLIC_API
                       EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/cbcrypto/visibility.h)

INSTALL(TARGETS cbcrypto
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    ADD_EXECUTABLE(cbcrypto_test
                   cbcrypto_test.cc
                   ${Memcached_SOURCE_DIR}/include/cbcrypto/cbcrypto.h)

    TARGET_LINK_LIBRARIES(cbcrypto_test
                          cbcrypto
                          platform
                          gtest gtest_main)

    ADD_TEST(NAME cbcrypto-test
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND cbcrypto_test)
endif(COUCHBASE_KV_BUILD_UNIT_TESTS)
