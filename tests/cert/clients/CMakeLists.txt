# Generate the client certificate signed by the Root CA
add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/client.csr
                   ${CMAKE_CURRENT_BINARY_DIR}/client.key
                   COMMAND ${OPENSSL_BINARY}
                   req
                   -new
                   -newkey rsa:2048
                   -keyout ${CMAKE_CURRENT_BINARY_DIR}/client.key
                   -config ${CMAKE_CURRENT_SOURCE_DIR}/config
                   -out ${CMAKE_CURRENT_BINARY_DIR}/client.csr
                   DEPENDS
                   ${CMAKE_CURRENT_SOURCE_DIR}/config
                   COMMENT "Generating Client signing request")

add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/client.cert
                   COMMAND ${OPENSSL_BINARY}
                   x509
                   -req
                   -in ${CMAKE_CURRENT_BINARY_DIR}/client.csr
                   -CA ${CMAKE_CURRENT_BINARY_DIR}/../root/ca_root.cert
                   -CAkey ${CMAKE_CURRENT_BINARY_DIR}/../root/ca_root.key
                   -CAcreateserial
                   -CAserial ${CMAKE_CURRENT_BINARY_DIR}/rootCA.srl
                   -extfile ${CMAKE_CURRENT_SOURCE_DIR}/ca.ext
                   -out ${CMAKE_CURRENT_BINARY_DIR}/client.cert
                   -days 365
                   COMMAND
                   ${OPENSSL_BINARY}
                   verify
                   -CAfile ${CMAKE_CURRENT_BINARY_DIR}/../root/ca_root.cert
                   ${CMAKE_CURRENT_BINARY_DIR}/client.cert
                   DEPENDS
                   Generate_kv_engine_X509_Test_Root_Certificate
                   ${CMAKE_CURRENT_SOURCE_DIR}/ca.ext
                   ${CMAKE_CURRENT_BINARY_DIR}/client.key
                   ${CMAKE_CURRENT_BINARY_DIR}/client.csr
                   COMMENT "Generate Client X.509 certificate")

add_custom_target(Generate_kv_engine_X509_Test_Client_Certificate
                  DEPENDS
                  ${CMAKE_CURRENT_BINARY_DIR}/client.key
                  ${CMAKE_CURRENT_BINARY_DIR}/client.cert)

add_custom_target(Dump_kv_engine_X509_Test_Client_Certificate
                  COMMAND
                  ${OPENSSL_BINARY}
                  x509
                  -in ${CMAKE_CURRENT_BINARY_DIR}/client.cert
                  -text
                  DEPENDS
                  Generate_kv_engine_X509_Test_Client_Certificate
                  )
