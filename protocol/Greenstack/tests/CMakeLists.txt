include_directories(AFTER ${gtest_SOURCE_DIR}/include)

if (NOT WIN32)
    set(GTEST_THREAD_LIBS pthread)
ENDIF (NOT WIN32)

include_directories(AFTER ${gtest_SOURCE_DIR})
add_executable(GreenstackUnitTests BitFieldPackTest.cc
        ExternalApiTest.cc FlexHeaderTest.cc
        ByteArrayReaderTest.cc ByteArrayWriterTest.cc
        RequestTest.cc ResponseTest.cc FrameTest.cc
        HelloTest.cc SaslTest.cc MutationTest.cc
        AssumeRoleTest.cc
        CreateBucketTest.cc DeleteBucketTest.cc ListBucketsTest.cc
        SelectBucketTest.cc GetTest.cc)
target_link_libraries(GreenstackUnitTests greenstack gtest gtest_main ${GTEST_THREAD_LIBS})
add_test(NAME GreenstackUnitTests COMMAND GreenstackUnitTests)
