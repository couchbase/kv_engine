# Generate an intermediate certificate signed by the Root CA
# used to sign other certificates
Generate_kv_engine_X509_Certificate(NAME intermediate
                                    CONFIG config
                                    ROOT ../root/ca_root
                                    EXTFILE ca.ext
                                    CA_TARGET Generate_kv_engine_X509_Test_Root_Certificate)

add_custom_target(Generate_kv_engine_X509_Test_Intermediate_Certificate
                  DEPENDS
                  Generate_kv_engine_X509_Test_Root_Certificate
                  ${CMAKE_CURRENT_BINARY_DIR}/intermediate.key
                  ${CMAKE_CURRENT_BINARY_DIR}/intermediate.cert)

add_custom_target(Dump_kv_engine_X509_Test_Intermediate_Certificate
                  COMMAND
                  ${OPENSSL_BINARY}
                  x509
                  -in ${CMAKE_CURRENT_BINARY_DIR}/intermediate.cert
                  -text
                  DEPENDS
                  Generate_kv_engine_X509_Test_Intermediate_Certificate
                  )
