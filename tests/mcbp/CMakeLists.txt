include_directories(AFTER ${gtest_SOURCE_DIR}/include)
ADD_DEFINITIONS(-DBUILDING_VALIDATORS_TEST)
add_executable(memcached_mcbp_test
               mcbp_test.cc
               mcbp_test_meta.cc
               mcbp_test_subdoc.cc
               mcbp_test_subdoc_xattr.cc
               xattr_blob_test.cc
               xattr_blob_validator_test.cc
               xattr_key_validator_test.cc
               ${PROJECT_SOURCE_DIR}/utilities/protocol2text.cc
               ${PROJECT_SOURCE_DIR}/utilities/subdoc_encoder.cc)
target_link_libraries(memcached_mcbp_test memcached_daemon platform
                      gtest gtest_main)
add_test(NAME memcached_mcbp_test COMMAND memcached_mcbp_test)

# GTest can be slow to compile (due to all the template / macro
# expansion).  Disabling optimization can speed up by ~30%.
target_compile_options(memcached_mcbp_test PRIVATE ${CB_CXX_FLAGS_NO_OPTIMIZE})
