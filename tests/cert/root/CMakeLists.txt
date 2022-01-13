# Generate the ROOT CA
add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/ca_root_encrypted.key
                   COMMAND
                   ${OPENSSL_BINARY}
                   rsa
                   -passout file:${CMAKE_CURRENT_SOURCE_DIR}/passphrase.txt
                   -des3
                   -in ${CMAKE_CURRENT_BINARY_DIR}/ca_root.key
                   -out ${CMAKE_CURRENT_BINARY_DIR}/ca_root_encrypted.key
                   DEPENDS passphrase.txt ${CMAKE_CURRENT_BINARY_DIR}/ca_root.key
                   COMMENT "Encrypt CA Root private key")

add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/ca_root.cert
                   ${CMAKE_CURRENT_BINARY_DIR}/ca_root.key
                   COMMAND
                   ${OPENSSL_BINARY}
                   req
                   -new
                   -x509
                   -days 365
                   -newkey rsa:2048
                   -keyout ${CMAKE_CURRENT_BINARY_DIR}/ca_root.key
                   -config ${CMAKE_CURRENT_SOURCE_DIR}/config
                   -out ${CMAKE_CURRENT_BINARY_DIR}/ca_root.cert
          DEPENDS
                   ${CMAKE_CURRENT_SOURCE_DIR}/config
                   COMMENT "Generate CA Root X.509 certificate")

add_custom_target(Generate_kv_engine_X509_Test_Root_Certificate
                  DEPENDS
                  ${CMAKE_CURRENT_BINARY_DIR}/ca_root.key
                  ${CMAKE_CURRENT_BINARY_DIR}/ca_root_encrypted.key
                  ${CMAKE_CURRENT_BINARY_DIR}/ca_root.cert)

add_custom_target(Dump_kv_engine_X509_Test_Root_Certificate
                  COMMAND
                  ${OPENSSL_BINARY}
                  x509
                  -in ${CMAKE_CURRENT_BINARY_DIR}/ca_root.cert
                  -text
                  DEPENDS
                  Generate_kv_engine_X509_Test_Root_Certificate
                  )
