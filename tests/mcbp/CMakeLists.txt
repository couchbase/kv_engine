include_directories(AFTER ${gtest_SOURCE_DIR}/include)
ADD_DEFINITIONS(-DBUILDING_VALIDATORS_TEST)
add_executable(memcached_mcbp_test mcbp_test.cc
               ${PROJECT_SOURCE_DIR}/daemon/mcbp_validators.cc)
target_link_libraries(memcached_mcbp_test platform gtest gtest_main)
add_test(NAME memcached_mcbp_test COMMAND memcached_mcbp_test)
