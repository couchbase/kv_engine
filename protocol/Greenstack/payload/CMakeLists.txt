file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/Greenstack/payload)

macro(Flatbuffers)
   add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/Greenstack/payload/${ARGV0}_generated.h
                      COMMAND
                         ${FLATC} -c
                                  -o ${CMAKE_CURRENT_BINARY_DIR}/Greenstack/payload
                                  ${CMAKE_CURRENT_SOURCE_DIR}/${ARGV0}.fbs
                      DEPENDS
                         ${CMAKE_CURRENT_SOURCE_DIR}/${ARGV0}.fbs
                         ${FLATC}
                      COMMENT "Generating code for ${ARGV0}")
    list(APPEND FLATBUFFER_SOURCES "${CMAKE_CURRENT_BINARY_DIR}/Greenstack/payload/${ARGV0}_generated.h")
endmacro(Flatbuffers)

Flatbuffers(AssumeRoleRequest)
Flatbuffers(CreateBucketRequest)
Flatbuffers(DeleteBucketRequest)
Flatbuffers(Document)
FlatBuffers(GetRequest)
FlatBuffers(GetResponse)
Flatbuffers(HelloRequest)
Flatbuffers(HelloResponse)
FlatBuffers(KeepaliveRequest)
Flatbuffers(ListBucketsResponse)
Flatbuffers(MutationRequest)
Flatbuffers(MutationResponse)
Flatbuffers(SaslAuthRequest)
Flatbuffers(SaslAuthResponse)
Flatbuffers(SelectBucketRequest)

add_custom_target(generate_flatbuffer_headers ALL
                  DEPENDS
                  ${FLATBUFFER_SOURCES})
