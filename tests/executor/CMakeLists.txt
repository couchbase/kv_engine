ADD_EXECUTABLE(memcached_executor_test
               ${PROJECT_SOURCE_DIR}/daemon/executor.cc
               ${PROJECT_SOURCE_DIR}/daemon/executor.h
               ${PROJECT_SOURCE_DIR}/daemon/executorpool.cc
               ${PROJECT_SOURCE_DIR}/daemon/executorpool.h
               ${PROJECT_SOURCE_DIR}/daemon/task.cc
               ${PROJECT_SOURCE_DIR}/daemon/task.h
               executor_test.cc)
TARGET_LINK_LIBRARIES(memcached_executor_test platform gtest)
ADD_TEST(NAME memcached-executor-tests
         WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
         COMMAND memcached_executor_test)
SET_TESTS_PROPERTIES(memcached-executor-tests PROPERTIES TIMEOUT 60)
