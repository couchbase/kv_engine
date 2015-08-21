include_directories(AFTER ${Memcached_BINARY_DIR}/protocol/Greenstack/payload)
include_directories(AFTER ${FLATBUFFERS_INCLUDE_DIR})

add_library(greenstack STATIC
        ${Memcached_SOURCE_DIR}/include/libgreenstack/Buffer.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/FlexHeader.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/Frame.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/Greenstack.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/Message.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/Opcodes.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/Reader.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/Request.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/Response.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/Status.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/Writer.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/core/Hello.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/core/Keepalive.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/core/SaslAuth.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/memcached/AssumeRole.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/memcached/Bucket.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/memcached/Cas.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/memcached/Compression.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/memcached/CreateBucket.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/memcached/Datatype.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/memcached/DeleteBucket.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/memcached/Document.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/memcached/DocumentInfo.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/memcached/Get.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/memcached/ListBuckets.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/memcached/Mutation.h
        ${Memcached_SOURCE_DIR}/include/libgreenstack/memcached/SelectBucket.h
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/AssumeRoleRequest.fbs
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/CreateBucketRequest.fbs
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/DeleteBucketRequest.fbs
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/Document.fbs
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/HelloRequest.fbs
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/HelloResponse.fbs
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/GetRequest.fbs
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/GetResponse.fbs
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/ListBucketsResponse.fbs
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/MutationRequest.fbs
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/MutationResponse.fbs
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/SaslAuthRequest.fbs
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/SaslAuthResponse.fbs
        ${Memcached_SOURCE_DIR}/protocol/Greenstack/payload/SelectBucketRequest.fbs

        core/Hello.cc
        core/Keepalive.cc
        core/SaslAuth.cc

        memcached/AssumeRole.cc
        memcached/Bucket.cc
        memcached/Cas.cc
        memcached/Compression.cc
        memcached/CreateBucket.cc
        memcached/Datatype.cc
        memcached/DeleteBucket.cc
        memcached/DocumentInfo.cc
        memcached/Get.cc
        memcached/ListBuckets.cc
        memcached/Mutation.cc
        memcached/SelectBucket.cc

        FlexHeader.cc
        Frame.cc
        Message.cc
        Opcodes.cc
        Request.cc
        Response.cc
        Status.cc
        )

add_dependencies(greenstack generate_flatbuffer_headers)
target_link_libraries(greenstack platform)
