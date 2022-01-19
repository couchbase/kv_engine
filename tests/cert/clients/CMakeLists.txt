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

add_custom_target(Generate_kv_engine_X509_Test_Client_Certificate
                  DEPENDS
                  ${CMAKE_CURRENT_BINARY_DIR}/john.key
                  ${CMAKE_CURRENT_BINARY_DIR}/john.cert
                  ${CMAKE_CURRENT_BINARY_DIR}/trond.cert
                  ${CMAKE_CURRENT_BINARY_DIR}/trond.key)

add_custom_target(Dump_kv_engine_X509_Test_Client_Certificate
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
                  DEPENDS
                  Generate_kv_engine_X509_Test_Client_Certificate
                  )
