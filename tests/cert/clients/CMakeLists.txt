# Generate two user certificates. john and trond signed by the Root CA
Generate_kv_engine_X509_Certificate(NAME trond
                                    CONFIG trond.config
                                    ROOT ../root/ca_root
                                    EXTFILE ca.ext
                                    CA_TARGET Generate_kv_engine_X509_Test_Root_Certificate)
Generate_kv_engine_X509_Certificate(NAME john
                                    CONFIG john.config
                                    ROOT ../root/ca_root
                                    EXTFILE ca.ext
                                    CA_TARGET Generate_kv_engine_X509_Test_Root_Certificate)
Generate_kv_engine_X509_Certificate(NAME jane
                                    CONFIG jane.config
                                    ROOT ../intermediate/intermediate
                                    EXTFILE ca.ext
                                    CA_TARGET Generate_kv_engine_X509_Test_Intermediate_Certificate)

Generate_kv_engine_X509_Certificate(NAME internal
                                    CONFIG internal.config
                                    ROOT ../root/ca_root
                                    EXTFILE internal-ca.ext
                                    CA_TARGET Generate_kv_engine_X509_Test_Root_Certificate)

add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/jane_chain.pem
                   COMMAND
                   ${CMAKE_COMMAND}
                   -E remove -f ${CMAKE_CURRENT_BINARY_DIR}/jane_chain.pem.in
                   COMMAND
                   ${CMAKE_COMMAND}
                   -D FILE1=${CMAKE_CURRENT_BINARY_DIR}/jane.cert
                   -D FILE2=${CMAKE_CURRENT_BINARY_DIR}/../intermediate/intermediate.cert
                   -D DESTINATION=${CMAKE_CURRENT_BINARY_DIR}/jane_chain.pem.in
                   -P ${CMAKE_CURRENT_SOURCE_DIR}/../ConcatinateFiles.cmake
                   COMMAND
                   ${CMAKE_COMMAND}
                   -E copy ${CMAKE_CURRENT_BINARY_DIR}/jane_chain.pem.in
                   ${CMAKE_CURRENT_BINARY_DIR}/jane_chain.pem
                   DEPENDS
                   ${CMAKE_CURRENT_BINARY_DIR}/jane.cert
                   COMMENT "Generate cerificate chain for jane")

add_custom_target(Generate_kv_engine_X509_Test_Client_Certificate
                  DEPENDS
                  ${CMAKE_CURRENT_BINARY_DIR}/jane.cert
                  ${CMAKE_CURRENT_BINARY_DIR}/jane.key
                  ${CMAKE_CURRENT_BINARY_DIR}/john.key
                  ${CMAKE_CURRENT_BINARY_DIR}/john.cert
                  ${CMAKE_CURRENT_BINARY_DIR}/trond.cert
                  ${CMAKE_CURRENT_BINARY_DIR}/trond.key
                  ${CMAKE_CURRENT_BINARY_DIR}/internal.cert
                  ${CMAKE_CURRENT_BINARY_DIR}/internal.key
                  ${CMAKE_CURRENT_BINARY_DIR}/jane_chain.pem)

add_custom_target(Dump_kv_engine_X509_Test_Client_Certificate
                  COMMAND
                  ${OPENSSL_BINARY}
                  x509
                  -in ${CMAKE_CURRENT_BINARY_DIR}/jane.cert
                  -text
                  COMMAND
                  ${OPENSSL_BINARY}
                  x509
                  -in ${CMAKE_CURRENT_BINARY_DIR}/john.cert
                  -text
                  COMMAND
                  ${OPENSSL_BINARY}
                  x509
                  -in ${CMAKE_CURRENT_BINARY_DIR}/trond.cert
                  -text
                  COMMAND
                  ${OPENSSL_BINARY}
                  x509
                  -in ${CMAKE_CURRENT_BINARY_DIR}/internal.cert
                  -text
                  DEPENDS
                  Generate_kv_engine_X509_Test_Client_Certificate
                  )
