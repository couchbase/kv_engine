ADD_LIBRARY(engine_utilities SHARED engine_error.cc)
TARGET_LINK_LIBRARIES(engine_utilities platform)

GENERATE_EXPORT_HEADER(engine_utilities
                       EXPORT_MACRO_NAME ENGINE_UTILITIES_PUBLIC_API
                       EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/memcached/engine_utilities_visibility.h)

INSTALL(TARGETS engine_utilities
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
