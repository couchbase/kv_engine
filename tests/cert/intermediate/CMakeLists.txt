# Generate an intermediate certificate signed by the Root CA
# used to sign other certificates
Generate_kv_engine_X509_Certificate(NAME intermediate
                                    CONFIG config
                                    ROOT ../root/ca_root
                                    EXTFILE ca.ext
                                    CA_TARGET Generate_kv_engine_X509_Test_Root_Certificate)

# Create a certificate chain file containing the intermediate certificate and
# the root certificate
add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/client_intermediate_ca.pem
                   COMMAND
                   ${CMAKE_COMMAND}
                   -E remove -f ${CMAKE_CURRENT_BINARY_DIR}/client_intermediate_ca.pem.in
                   COMMAND
                   ${CMAKE_COMMAND}
                   -D FILE1=${CMAKE_CURRENT_BINARY_DIR}/../intermediate/intermediate.cert
                   -D FILE2=${CMAKE_CURRENT_BINARY_DIR}/../root/ca_root.cert
                   -D DESTINATION=${CMAKE_CURRENT_BINARY_DIR}/client_intermediate_ca.pem.in
                   -P ${CMAKE_CURRENT_SOURCE_DIR}/../ConcatinateFiles.cmake
                   COMMAND
                   ${CMAKE_COMMAND}
                   -E copy ${CMAKE_CURRENT_BINARY_DIR}/client_intermediate_ca.pem.in
                   ${CMAKE_CURRENT_BINARY_DIR}/client_intermediate_ca.pem
                   DEPENDS
                   ${CMAKE_CURRENT_BINARY_DIR}/intermediate.cert
                   COMMENT "Generate cerificate chain for node1")

add_custom_target(Generate_kv_engine_X509_Test_Intermediate_Certificate
                  DEPENDS
                  Generate_kv_engine_X509_Test_Root_Certificate
                  ${CMAKE_CURRENT_BINARY_DIR}/intermediate.key
                  ${CMAKE_CURRENT_BINARY_DIR}/intermediate.cert
                  ${CMAKE_CURRENT_BINARY_DIR}/client_intermediate_ca.pem)

add_custom_target(Dump_kv_engine_X509_Test_Intermediate_Certificate
                  COMMAND
                  ${OPENSSL_BINARY}
                  x509
                  -in ${CMAKE_CURRENT_BINARY_DIR}/intermediate.cert
                  -text
                  DEPENDS
                  Generate_kv_engine_X509_Test_Intermediate_Certificate
                  )
