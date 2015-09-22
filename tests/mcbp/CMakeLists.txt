include_directories(AFTER ${gtest_SOURCE_DIR}/include)
ADD_DEFINITIONS(-DBUILDING_VALIDATORS_TEST)
add_executable(memcached_mcbp_test
               mcbp_test.cc
               mcbp_test_subdoc.cc
               ${PROJECT_SOURCE_DIR}/daemon/mcbp_validators.cc
               ${PROJECT_SOURCE_DIR}/daemon/subdocument_traits.cc
               ${PROJECT_SOURCE_DIR}/daemon/subdocument_validators.cc
               ${PROJECT_SOURCE_DIR}/utilities/protocol2text.cc
               ${PROJECT_SOURCE_DIR}/utilities/subdoc_encoder.cc)
target_link_libraries(memcached_mcbp_test platform gtest gtest_main)
add_test(NAME memcached_mcbp_test COMMAND memcached_mcbp_test)

# GTest can be slow to compile (due to all the template / macro
# expansion).  Disabling optimization can speed up by ~30%.
target_compile_options(memcached_mcbp_test PRIVATE ${CMAKE_CXX_FLAGS_DEBUG})
