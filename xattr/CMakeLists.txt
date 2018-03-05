ADD_LIBRARY(xattr SHARED
            ${PROJECT_SOURCE_DIR}/include/xattr/blob.h
            ${PROJECT_SOURCE_DIR}/include/xattr/key_validator.h
            ${PROJECT_SOURCE_DIR}/include/xattr/utils.h
            blob.cc
            key_validator.cc
            utils.cc)

SET_TARGET_PROPERTIES(xattr PROPERTIES SOVERSION 0.1.0)
TARGET_LINK_LIBRARIES(xattr cbcompress cJSON platform)
GENERATE_EXPORT_HEADER(xattr
                       EXPORT_MACRO_NAME XATTR_PUBLIC_API
                       EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/xattr/visibility.h)

INSTALL(TARGETS xattr
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
