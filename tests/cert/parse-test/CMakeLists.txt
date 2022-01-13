# Generate the certificate we'll be using for the parse-tests
add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/parse-test.csr
                   ${CMAKE_CURRENT_BINARY_DIR}/parse-test.key
                   COMMAND ${OPENSSL_BINARY}
                   req
                   -new
                   -nodes
                   -newkey rsa:2048
                   -keyout ${CMAKE_CURRENT_BINARY_DIR}/parse-test.key
                   -out ${CMAKE_CURRENT_BINARY_DIR}/parse-test.csr
                   -config ${CMAKE_CURRENT_SOURCE_DIR}/config
                   DEPENDS
                   ${CMAKE_CURRENT_SOURCE_DIR}/config
                   COMMENT "Generate parse-config signing request")

add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/parse-test.cert
                   COMMAND ${OPENSSL_BINARY}
                   x509
                   -req
                   -in ${CMAKE_CURRENT_BINARY_DIR}/parse-test.csr
                   -CA ${CMAKE_CURRENT_BINARY_DIR}/../root/ca_root.cert
                   -CAkey ${CMAKE_CURRENT_BINARY_DIR}/../root/ca_root.key
                   -CAcreateserial
                   -CAserial ${CMAKE_CURRENT_BINARY_DIR}/rootCA.srl
                   -extfile ${CMAKE_CURRENT_SOURCE_DIR}/ca.ext
                   -out ${CMAKE_CURRENT_BINARY_DIR}/parse-test.cert
                   -days 365
                   DEPENDS
                   ${CMAKE_CURRENT_BINARY_DIR}/parse-test.csr
                   ${CMAKE_CURRENT_BINARY_DIR}/../root/ca_root.cert
                   ${CMAKE_CURRENT_BINARY_DIR}/../root/ca_root.key
                   ${CMAKE_CURRENT_SOURCE_DIR}/ca.ext
                   COMMENT "Generate parse-config X.509 certificate")

add_custom_target(Generate_kv_engine_X509_Test_Parse_Certificate
                  DEPENDS
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