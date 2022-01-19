# Generate the certificate we'll be using for the parse-tests
Generate_kv_engine_X509_Certificate(NAME parse-test
                                    CONFIG config
                                    ROOT ../root/ca_root
                                    EXTFILE ca.ext
                                    CA_TARGET Generate_kv_engine_X509_Test_Root_Certificate)

add_custom_target(Generate_kv_engine_X509_Test_Parse_Certificate
                  DEPENDS
                  Generate_kv_engine_X509_Test_Root_Certificate
                  ${CMAKE_CURRENT_BINARY_DIR}/parse-test.cert
                  ${CMAKE_CURRENT_BINARY_DIR}/parse-test.key)

add_custom_target(Dump_kv_engine_X509_Test_Parse_Certificate
                  COMMAND
                  ${OPENSSL_BINARY}
                  x509
                  -in ${CMAKE_CURRENT_BINARY_DIR}/parse-test.cert
                  -text
                  DEPENDS
                  Generate_kv_engine_X509_Test_Parse_Certificate
                  )