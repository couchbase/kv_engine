ADD_LIBRARY(mcd_util SHARED
            config_parser.c
            engine_loader.c
            extension_loggers.c
            protocol2text.c
            util.c)
TARGET_LINK_LIBRARIES(mcd_util platform)
SET_TARGET_PROPERTIES(mcd_util PROPERTIES SOVERSION 1.0.0)
INSTALL(TARGETS mcd_util
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/memcached
        ARCHIVE DESTINATION lib/memcached)

ADD_EXECUTABLE(utilities_testapp
               config_parser.c
               util.c
               util_test.cc)
TARGET_LINK_LIBRARIES(utilities_testapp gtest gtest_main platform)
ADD_TEST(NAME memcached-utilities-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND utilities_testapp)
