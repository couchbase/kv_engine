ADD_LIBRARY(mcd_util SHARED
            config_parser.cc
            engine_loader.cc
            extension_loggers.cc
            protocol2text.cc
            util.cc)
TARGET_LINK_LIBRARIES(mcd_util platform)
SET_TARGET_PROPERTIES(mcd_util PROPERTIES SOVERSION 1.0.0)
INSTALL(TARGETS mcd_util
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

ADD_EXECUTABLE(utilities_testapp
               config_parser.cc
               string_utilities.cc
               util.cc
               util_test.cc)
TARGET_LINK_LIBRARIES(utilities_testapp gtest gtest_main gmock platform)
ADD_TEST(NAME memcached-utilities-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND utilities_testapp)
