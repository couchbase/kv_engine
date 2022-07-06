set(OPENSSL_BINARY
    ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/openssl
    CACHE
    PATH
    "Path to the openssl binary in use (from cbdep)"
    FORCE)

# Unfortunately openssl don't provide a -quiet option, but will always
# spam the terminal with progress output. We can "easily" work around
# that on *nix by creating a wrapper script and redirect output/error
# (we could probably do the same on Windows, but given that we don't
# have too many developers using windows the verbosity is probably
# not a huge problem)
if (UNIX)
    configure_file(${CMAKE_CURRENT_SOURCE_DIR}/openssl_wrapper.sh.in
                   ${CMAKE_CURRENT_BINARY_DIR}/openssl_wrapper
                   @ONLY)
    set(QUIET_OPENSSL_BINARY
        ${CMAKE_CURRENT_BINARY_DIR}/openssl_wrapper
        CACHE
        PATH
        "Path to the openssl binary in use (from cbdep)"
        FORCE)
else ()
    set(QUIET_OPENSSL_BINARY
        ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/openssl
        CACHE
        PATH
        "Path to the openssl binary in use (from cbdep)"
        FORCE)
endif ()

MACRO (Generate_kv_engine_X509_Certificate)
    cmake_parse_arguments(Cert "" "NAME;CONFIG;ROOT;EXTFILE;CA_TARGET" "" ${ARGN})
    add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.csr
                       ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.key
                       COMMAND
                       ${CMAKE_COMMAND}
                       -E remove -f
                       ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.key
                       ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.csr
                       ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.key.tmp
                       ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.csr.tmp
                       COMMAND ${QUIET_OPENSSL_BINARY}
                       req
                       -new
                       -newkey rsa:2048
                       -keyout ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.key.tmp
                       -config ${CMAKE_CURRENT_SOURCE_DIR}/${Cert_CONFIG}
                       -out ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.csr.tmp
                       COMMAND
                       ${CMAKE_COMMAND}
                       -E copy ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.key.tmp
                       ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.key
                       COMMAND
                       ${CMAKE_COMMAND}
                       -E copy ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.csr.tmp
                       ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.csr
                       DEPENDS
                       ${CMAKE_CURRENT_SOURCE_DIR}/${Cert_CONFIG}
                       COMMENT "Generating ${Cert_NAME} signing request")

    add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.cert
                       COMMAND
                       ${CMAKE_COMMAND}
                       -E remove -f
                       ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.cert
                       ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.cert.tmp
                       COMMAND ${QUIET_OPENSSL_BINARY}
                       x509
                       -req
                       -in ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.csr
                       -CA ${CMAKE_CURRENT_BINARY_DIR}/${Cert_ROOT}.cert
                       -CAkey ${CMAKE_CURRENT_BINARY_DIR}/${Cert_ROOT}.key
                       -set_serial 1
                       -extfile ${CMAKE_CURRENT_SOURCE_DIR}/${Cert_EXTFILE}
                       -out ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.cert.tmp
                       -days 365
                       COMMAND
                       ${CMAKE_COMMAND}
                       -E copy ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.cert.tmp
                       ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.cert
                       DEPENDS
                       ${Cert_CA_TARGET}
                       ${CMAKE_CURRENT_SOURCE_DIR}/${Cert_EXTFILE}
                       ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.key
                       ${CMAKE_CURRENT_BINARY_DIR}/${Cert_NAME}.csr
                       COMMENT "Generate ${Cert_NAME} X.509 certificate")
endmacro()

add_subdirectory(root)
add_subdirectory(intermediate)
add_subdirectory(clients)
add_subdirectory(parse-test)
add_subdirectory(servers)

add_custom_target(Generate_kv_engine_X509_Test_Certificates
                  DEPENDS
                  Generate_kv_engine_X509_Test_Root_Certificate
                  Generate_kv_engine_X509_Test_Intermediate_Certificate
                  Generate_kv_engine_X509_Test_Client_Certificate
                  Generate_kv_engine_X509_Test_Parse_Certificate
                  Generate_kv_engine_X509_Test_Node_Certificate)
