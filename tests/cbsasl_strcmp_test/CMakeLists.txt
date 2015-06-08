ADD_EXECUTABLE(cbsasl_strcmp_test strcmp_test.c
  ${Memcached_SOURCE_DIR}/include/cbsasl/cbsasl.h
  ${Memcached_SOURCE_DIR}/include/cbsasl/visibility.h
  ${Memcached_SOURCE_DIR}/cbsasl/strcmp.c)
TARGET_LINK_LIBRARIES(cbsasl_strcmp_test platform)
ADD_TEST(NAME cbsasl-strcmp
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND cbsasl_strcmp_test)
