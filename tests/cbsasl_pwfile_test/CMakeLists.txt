ADD_EXECUTABLE(cbsasl_pwfile_test pwfile_test.cc
               ${Memcached_SOURCE_DIR}/cbsasl/log.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwfile.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwfile.h)
TARGET_LINK_LIBRARIES(cbsasl_pwfile_test gtest gtest_main platform)
ADD_TEST(NAME cbsasl-pwfile
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbsasl_pwfile_test)
