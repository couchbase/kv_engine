set(OPENSSL_BINARY
    ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}/openssl
    CACHE
    PATH
    "Path to the openssl binary in use (from cbdep)"
    FORCE)

add_subdirectory(root)
add_subdirectory(clients)
add_subdirectory(parse-test)

add_custom_target(Generate_kv_engine_X509_Test_Certificates
                  DEPENDS
                  Generate_kv_engine_X509_Test_Root_Certificate
                  Generate_kv_engine_X509_Test_Client_Certificate
                  Generate_kv_engine_X509_Test_Parse_Certificate)
