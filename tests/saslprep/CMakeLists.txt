INCLUDE_DIRECTORIES(AFTER ${ICU_INCLUDE_DIR})
ADD_EXECUTABLE(cbsasl_saslprep_test
               ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/stringutils.cc
               ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/stringutils.h
               saslprep_test.cc)
TARGET_LINK_LIBRARIES(cbsasl_saslprep_test platform gtest gtest_main)
ADD_TEST(NAME cbsasl-saslprep-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbsasl_saslprep_test)
