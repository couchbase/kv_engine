# Generate a server certificate signed by the Root CA
add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/node1.csr
                   ${CMAKE_CURRENT_BINARY_DIR}/node1.key
                   COMMAND ${OPENSSL_BINARY}
                   req
                   -new
                   -newkey rsa:2048
                   -keyout ${CMAKE_CURRENT_BINARY_DIR}/node1.key
                   -config ${CMAKE_CURRENT_SOURCE_DIR}/config
                   -out ${CMAKE_CURRENT_BINARY_DIR}/node1.csr
                   DEPENDS
                   ${CMAKE_CURRENT_SOURCE_DIR}/config
                   COMMENT "Generating node1 signing request")

add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/node1.cert
                   COMMAND ${OPENSSL_BINARY}
                   x509
                   -req
                   -in ${CMAKE_CURRENT_BINARY_DIR}/node1.csr
                   -CA ${CMAKE_CURRENT_BINARY_DIR}/../root/ca_root.cert
                   -CAkey ${CMAKE_CURRENT_BINARY_DIR}/../root/ca_root.key
                   -CAcreateserial
                   -CAserial ${CMAKE_CURRENT_BINARY_DIR}/rootCA.srl
                   -extfile ${CMAKE_CURRENT_SOURCE_DIR}/ca.ext
                   -out ${CMAKE_CURRENT_BINARY_DIR}/node1.cert
                   -days 365
                   COMMAND
                   ${OPENSSL_BINARY}
                   verify
                   -CAfile ${CMAKE_CURRENT_BINARY_DIR}/../root/ca_root.cert
                   ${CMAKE_CURRENT_BINARY_DIR}/node1.cert
                   DEPENDS
		   Generate_kv_engine_X509_Test_Root_Certificate
                   ${CMAKE_CURRENT_SOURCE_DIR}/ca.ext
                   ${CMAKE_CURRENT_BINARY_DIR}/node1.key
                   ${CMAKE_CURRENT_BINARY_DIR}/node1.csr
                   COMMENT "Generate node1 X.509 certificate")

add_custom_target(Generate_kv_engine_X509_Test_Node_Certificate
                  DEPENDS
                  ${CMAKE_CURRENT_BINARY_DIR}/node1.key
                  ${CMAKE_CURRENT_BINARY_DIR}/node1.cert)

add_custom_target(Dump_kv_engine_X509_Test_Node_Certificate
                  COMMAND
                  ${OPENSSL_BINARY}
                  x509
                  -in ${CMAKE_CURRENT_BINARY_DIR}/node1.cert
                  -text
                  DEPENDS
                  Generate_kv_engine_X509_Test_Node_Certificate
                  )
