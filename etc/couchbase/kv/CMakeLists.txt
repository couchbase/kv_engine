add_subdirectory(error_maps)
add_subdirectory(opcode-attributes.d)
add_subdirectory(serverless)

install(FILES opcode-attributes.json DESTINATION etc/couchbase/kv)
