ADD_LIBRARY(xattr STATIC
            ${PROJECT_SOURCE_DIR}/include/xattr/blob.h
            ${PROJECT_SOURCE_DIR}/include/xattr/key_validator.h
            ${PROJECT_SOURCE_DIR}/include/xattr/utils.h
            blob.cc
            key_validator.cc
            utils.cc)
SET_TARGET_PROPERTIES(xattr PROPERTIES POSITION_INDEPENDENT_CODE 1)
target_link_libraries(xattr PRIVATE cbcompress platform JSON_checker)
add_sanitizers(xattr)
kv_enable_pch(xattr)
