# Generate a server certificate signed by the intermediate CA
Generate_kv_engine_X509_Certificate(NAME node1
                                    CONFIG config
                                    ROOT ../intermediate/intermediate
                                    EXTFILE ca.ext
                                    CA_TARGET Generate_kv_engine_X509_Test_Intermediate_Certificate)

# Create a chain by copying all certs into a single file
add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/chain.cert
                   COMMAND
                   ${CMAKE_COMMAND}
                   -E remove -f ${CMAKE_CURRENT_BINARY_DIR}/chain.cert.in
                   COMMAND
                   ${CMAKE_COMMAND}
                   -D FILE1=${CMAKE_CURRENT_BINARY_DIR}/node1.cert
                   -D FILE2=${CMAKE_CURRENT_BINARY_DIR}/../intermediate/intermediate.cert
                   -D DESTINATION=${CMAKE_CURRENT_BINARY_DIR}/chain.cert.in
                   -P ${CMAKE_CURRENT_SOURCE_DIR}/../ConcatinateFiles.cmake
                   COMMAND
                   ${CMAKE_COMMAND}
                   -E copy ${CMAKE_CURRENT_BINARY_DIR}/chain.cert.in
                   ${CMAKE_CURRENT_BINARY_DIR}/chain.cert
                   DEPENDS
                   ${CMAKE_CURRENT_BINARY_DIR}/node1.cert
                   COMMENT "Generate cerificate chain for node1")

add_custom_target(Generate_kv_engine_X509_Test_Node_Certificate
                  DEPENDS
                  ${CMAKE_CURRENT_BINARY_DIR}/node1.key
                  ${CMAKE_CURRENT_BINARY_DIR}/chain.cert)

add_custom_target(Dump_kv_engine_X509_Test_Node_Certificate
                  COMMAND
                  ${OPENSSL_BINARY}
                  x509
                  -in ${CMAKE_CURRENT_BINARY_DIR}/node1.cert
                  -text
                  DEPENDS
                  Generate_kv_engine_X509_Test_Node_Certificate
                  )
