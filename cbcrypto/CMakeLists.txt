ADD_LIBRARY(cbcrypto SHARED
            cbcrypto.cc
            ${PROJECT_SOURCE_DIR}/include/cbcrypto/cbcrypto.h)
SET_TARGET_PROPERTIES(cbcrypto PROPERTIES SOVERSION 1.0.0)

if (APPLE)
    # Apple is using the Common Crypto Framework for all crypto functions
    TARGET_LINK_LIBRARIES(cbcrypto cJSON platform)
elseif (WIN32)
    # Use the OpenSSL ones for all other platforms
    TARGET_LINK_LIBRARIES(cbcrypto cJSON platform Bcrypt)
else()
    # Use the OpenSSL ones for all other platforms
    TARGET_LINK_LIBRARIES(cbcrypto cJSON platform ${OPENSSL_LIBRARIES})
endif ()

GENERATE_EXPORT_HEADER(cbcrypto
                       EXPORT_MACRO_NAME CBCRYPTO_PUBLIC_API
                       EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/cbcrypto/visibility.h)

INSTALL(TARGETS cbcrypto
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
