ADD_LIBRARY(cbcrypto SHARED
            cbcrypto.cc
            ${PROJECT_SOURCE_DIR}/include/cbcrypto/cbcrypto.h)
SET_TARGET_PROPERTIES(cbcrypto PROPERTIES SOVERSION 1.0.0)
TARGET_LINK_LIBRARIES(cbcrypto cJSON platform ${OPENSSL_LIBRARIES})

GENERATE_EXPORT_HEADER(cbcrypto
                       EXPORT_MACRO_NAME CBCRYPTO_PUBLIC_API
                       EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/cbcrypto/visibility.h)

INSTALL(TARGETS cbcrypto
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
