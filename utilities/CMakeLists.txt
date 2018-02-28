add_library(mcd_util SHARED
            ${PROJECT_SOURCE_DIR}/include/memcached/config_parser.h
            ${PROJECT_SOURCE_DIR}/include/memcached/util.h
            config_parser.cc
            engine_loader.cc
            engine_loader.h
            protocol2text.cc
            protocol2text.h
            string_utilities.cc
            string_utilities.h
            terminate_handler.cc
            terminate_handler.h
            util.cc)
target_link_libraries(mcd_util memcached_logger engine_utilities platform)
set_target_properties(mcd_util PROPERTIES SOVERSION 1.0.0)

generate_export_header(mcd_util
                       EXPORT_MACRO_NAME MCD_UTIL_PUBLIC_API
                       EXPORT_FILE_NAME ${Memcached_BINARY_DIR}/include/memcached/mcd_util-visibility.h)

install(TARGETS mcd_util
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    add_executable(utilities_testapp util_test.cc)
    target_link_libraries(utilities_testapp
                          mcd_util
                          platform
                          gtest
                          gtest_main
                          gmock)
    add_test(NAME memcached-utilities-tests
             WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
             COMMAND utilities_testapp)
endif (COUCHBASE_KV_BUILD_UNIT_TESTS)
