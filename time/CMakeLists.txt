ADD_LIBRARY(mcd_time SHARED
            isotime.cc
            ${PROJECT_SOURCE_DIR}/include/memcached/isotime.h)
SET_TARGET_PROPERTIES(mcd_time PROPERTIES SOVERSION 1.0.0)
TARGET_LINK_LIBRARIES(mcd_time platform)

ADD_EXECUTABLE(memcached_timestamp_test timestamp_test.cc
               ${PROJECT_SOURCE_DIR}/include/memcached/isotime.h)
TARGET_LINK_LIBRARIES(memcached_timestamp_test mcd_time)
ADD_TEST(NAME memcached-timestamp-test
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_timestamp_test)

INSTALL(TARGETS mcd_time
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
