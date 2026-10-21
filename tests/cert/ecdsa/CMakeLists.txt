# Generate a separate ECDSA hierarchy (root CA, server and client
# certificate) using P-521 keys signed with SHA-512. This meets the
# requirements of all of the OpenSSL security levels (0-5), which isn't
# feasible with RSA as level 5 would require 15360 bit RSA keys.
add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/ca_root.cert
                   ${CMAKE_CURRENT_BINARY_DIR}/ca_root.key
                   COMMAND
                   ${CMAKE_COMMAND}
                   -E remove -f ${CMAKE_CURRENT_BINARY_DIR}/ca_root.cert ${CMAKE_CURRENT_BINARY_DIR}/ca_root.key
                   COMMAND
                   ${QUIET_OPENSSL_BINARY}
                   req
                   -new
                   -x509
                   -days 365
                   -newkey ec
                   -pkeyopt ec_paramgen_curve:P-521
                   -sha512
                   -keyout ${CMAKE_CURRENT_BINARY_DIR}/ca_root.key.tmp
                   -config ${CMAKE_CURRENT_SOURCE_DIR}/../root/config
                   -out ${CMAKE_CURRENT_BINARY_DIR}/ca_root.cert.tmp
                   COMMAND
                   ${CMAKE_COMMAND}
                   -E copy ${CMAKE_CURRENT_BINARY_DIR}/ca_root.key.tmp
                   ${CMAKE_CURRENT_BINARY_DIR}/ca_root.key
                   COMMAND
                   ${CMAKE_COMMAND}
                   -E copy ${CMAKE_CURRENT_BINARY_DIR}/ca_root.cert.tmp
                   ${CMAKE_CURRENT_BINARY_DIR}/ca_root.cert
                   DEPENDS
                   ${CMAKE_CURRENT_SOURCE_DIR}/../root/config
                   COMMENT "Generate ECDSA CA Root X.509 certificate")

add_custom_target(Generate_kv_engine_X509_Test_ECDSA_Root_Certificate
                  DEPENDS
                  ${CMAKE_CURRENT_BINARY_DIR}/ca_root.key
                  ${CMAKE_CURRENT_BINARY_DIR}/ca_root.cert)

Generate_kv_engine_X509_Certificate(NAME node1
                                    CONFIG ../servers/config
                                    ROOT ca_root
                                    EXTFILE ../servers/ca.ext
                                    CURVE P-521
                                    DIGEST sha512
                                    CA_TARGET Generate_kv_engine_X509_Test_ECDSA_Root_Certificate)

Generate_kv_engine_X509_Certificate(NAME trond
                                    CONFIG ../clients/trond.config
                                    ROOT ca_root
                                    EXTFILE ../clients/ca.ext
                                    CURVE P-521
                                    DIGEST sha512
                                    CA_TARGET Generate_kv_engine_X509_Test_ECDSA_Root_Certificate)

add_custom_target(Generate_kv_engine_X509_Test_ECDSA_Certificates
                  DEPENDS
                  Generate_kv_engine_X509_Test_ECDSA_Root_Certificate
                  ${CMAKE_CURRENT_BINARY_DIR}/node1.key
                  ${CMAKE_CURRENT_BINARY_DIR}/node1.cert
                  ${CMAKE_CURRENT_BINARY_DIR}/trond.key
                  ${CMAKE_CURRENT_BINARY_DIR}/trond.cert)
