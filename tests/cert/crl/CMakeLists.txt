get_filename_component(ROOT_CA_BINARY_DIR
                       "${CMAKE_CURRENT_BINARY_DIR}/../root" ABSOLUTE)
get_filename_component(INTERMEDIATE_BINARY_DIR
                       "${CMAKE_CURRENT_BINARY_DIR}/../intermediate" ABSOLUTE)
get_filename_component(CLIENTS_BINARY_DIR
                       "${CMAKE_CURRENT_BINARY_DIR}/../clients" ABSOLUTE)

find_program(PYTHON3_EXECUTABLE python3 REQUIRED)

# Helper macro that wires up one CRL generation command.
# NAME           - base name for output .crl and its cnf / db directory
# CA_CERT        - full path to the signing CA certificate
# CA_KEY         - full path to the signing CA private key
# CA_TARGET      - cmake target that produces the CA cert/key
# CERT_TARGET    - cmake target that produces the certificate being revoked
#                  (only needed when REVOKED_SERIAL is set)
# REVOKED_SERIAL - optional hex serial to mark revoked in this CRL
# CRL_DAYS       - optional validity period in days; use 0 for an expired CRL
#                  (0 is mapped to -crlsec 1 since OpenSSL 3 rejects -crldays 0)
macro(Generate_kv_engine_CRL)
    cmake_parse_arguments(CRL
        "NOT_YET_VALID"
        "NAME;CA_CERT;CA_KEY;CA_TARGET;CERT_TARGET;REVOKED_SERIAL;CRL_DAYS"
        ""
        ${ARGN})

    set(CRL_CNF  ${CMAKE_CURRENT_BINARY_DIR}/${CRL_NAME}.cnf)
    set(CRL_FILE ${CMAKE_CURRENT_BINARY_DIR}/${CRL_NAME}.crl)

    # Bake absolute paths into the CA config at cmake-configure time.
    set(CA_CERT  ${CRL_CA_CERT})
    set(CA_KEY   ${CRL_CA_KEY})
    set(DB_DIR   ${CMAKE_CURRENT_BINARY_DIR}/${CRL_NAME}_db)
    configure_file(ca.cnf.in ${CRL_CNF} @ONLY)

    # Build up the optional -D REVOKED_SERIAL=... argument.
    if (CRL_REVOKED_SERIAL)
        set(_revoke_arg "-DREVOKED_SERIAL=${CRL_REVOKED_SERIAL}")
    else ()
        set(_revoke_arg "")
    endif ()

    # Build up the optional -D CRL_DAYS=... argument.
    if (DEFINED CRL_CRL_DAYS)
        set(_crl_days_arg "-DCRL_DAYS=${CRL_CRL_DAYS}")
    else ()
        set(_crl_days_arg "")
    endif ()

    # Build up the optional -D CRL_NOT_YET_VALID=... argument.
    if (CRL_NOT_YET_VALID)
        set(_not_yet_valid_arg "-DCRL_NOT_YET_VALID=TRUE")
    else ()
        set(_not_yet_valid_arg "")
    endif ()

    # Determine which cmake targets this command depends on.
    set(_depends ${CRL_CA_TARGET} ${CRL_CNF})
    if (CRL_CERT_TARGET)
        list(APPEND _depends ${CRL_CERT_TARGET})
    endif ()

    add_custom_command(
        OUTPUT ${CRL_FILE}
        COMMAND ${CMAKE_COMMAND}
                -D OPENSSL=${OPENSSL_BINARY}
                -D CA_CNF=${CRL_CNF}
                -D DB_DIR=${CMAKE_CURRENT_BINARY_DIR}/${CRL_NAME}_db
                -D OUTPUT=${CRL_FILE}
                ${_revoke_arg}
                ${_crl_days_arg}
                ${_not_yet_valid_arg}
                -P ${CMAKE_CURRENT_SOURCE_DIR}/GenerateCrl.cmake
        DEPENDS ${_depends}
        COMMENT "Generate CRL: ${CRL_NAME}.crl")
endmacro()

# Root CA — no revocations
Generate_kv_engine_CRL(
    NAME      root_ca
    CA_CERT   ${ROOT_CA_BINARY_DIR}/ca_root.cert
    CA_KEY    ${ROOT_CA_BINARY_DIR}/ca_root.key
    CA_TARGET Generate_kv_engine_X509_Test_Root_Certificate)

# Root CA — john's certificate (serial 01) revoked
Generate_kv_engine_CRL(
    NAME           root_ca_john_revoked
    CA_CERT        ${ROOT_CA_BINARY_DIR}/ca_root.cert
    CA_KEY         ${ROOT_CA_BINARY_DIR}/ca_root.key
    CA_TARGET      Generate_kv_engine_X509_Test_Root_Certificate
    CERT_TARGET    Generate_kv_engine_X509_Test_Client_Certificate
    REVOKED_SERIAL 01)

# Intermediate CA — no revocations
Generate_kv_engine_CRL(
    NAME      intermediate_ca
    CA_CERT   ${INTERMEDIATE_BINARY_DIR}/intermediate.cert
    CA_KEY    ${INTERMEDIATE_BINARY_DIR}/intermediate.key
    CA_TARGET Generate_kv_engine_X509_Test_Intermediate_Certificate)

# Intermediate CA — jane's certificate (serial 01) revoked
Generate_kv_engine_CRL(
    NAME           intermediate_ca_jane_revoked
    CA_CERT        ${INTERMEDIATE_BINARY_DIR}/intermediate.cert
    CA_KEY         ${INTERMEDIATE_BINARY_DIR}/intermediate.key
    CA_TARGET      Generate_kv_engine_X509_Test_Intermediate_Certificate
    CERT_TARGET    Generate_kv_engine_X509_Test_Client_Certificate
    REVOKED_SERIAL 01)

# Intermediate CA — CRL valid for 0 days so it is expired the moment it is
# built.  Used to test that Permissive tolerates an expired CRL and that
# Strict/Require reject it.
Generate_kv_engine_CRL(
    NAME      intermediate_ca_expired
    CA_CERT   ${INTERMEDIATE_BINARY_DIR}/intermediate.cert
    CA_KEY    ${INTERMEDIATE_BINARY_DIR}/intermediate.key
    CA_TARGET Generate_kv_engine_X509_Test_Intermediate_Certificate
    CRL_DAYS  0)

# Intermediate CA — CRL whose thisUpdate is pinned to 2049-12-31 so it is
# not yet valid at any realistic build time (X509_V_ERR_CRL_NOT_YET_VALID).
# Used to test that Permissive tolerates this and that Strict/Require reject it.
Generate_kv_engine_CRL(
    NAME          intermediate_ca_not_yet_valid
    CA_CERT       ${INTERMEDIATE_BINARY_DIR}/intermediate.cert
    CA_KEY        ${INTERMEDIATE_BINARY_DIR}/intermediate.key
    CA_TARGET     Generate_kv_engine_X509_Test_Intermediate_Certificate
    NOT_YET_VALID)

# Intermediate CA — CRL with a corrupted signature. Used to test that all
# policies handle X509_V_ERR_CRL_SIGNATURE_FAILURE correctly:
#   Permissive/Strict: reject silently (falls through to preverify_ok=0)
#   Require:           reject and invoke the error callback
add_custom_command(
    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca_bad_signature.crl
    COMMAND ${CMAKE_COMMAND}
            -D OPENSSL=${OPENSSL_BINARY}
            -D PYTHON3=${PYTHON3_EXECUTABLE}
            -D INPUT=${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca.crl
            -D OUTPUT=${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca_bad_signature.crl
            -P ${CMAKE_CURRENT_SOURCE_DIR}/CorruptCrl.cmake
    DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca.crl
    COMMENT "Generate bad-signature CRL: intermediate_ca_bad_signature.crl")

add_custom_target(Generate_kv_engine_X509_Test_CRL
                  DEPENDS
                  ${CMAKE_CURRENT_BINARY_DIR}/root_ca.crl
                  ${CMAKE_CURRENT_BINARY_DIR}/root_ca_john_revoked.crl
                  ${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca.crl
                  ${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca_jane_revoked.crl
                  ${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca_expired.crl
                  ${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca_not_yet_valid.crl
                  ${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca_bad_signature.crl)

add_custom_target(Dump_kv_engine_X509_Test_CRL
                  COMMAND ${OPENSSL_BINARY} crl
                          -in ${CMAKE_CURRENT_BINARY_DIR}/root_ca.crl
                          -text -noout
                  COMMAND ${OPENSSL_BINARY} crl
                          -in ${CMAKE_CURRENT_BINARY_DIR}/root_ca_john_revoked.crl
                          -text -noout
                  COMMAND ${OPENSSL_BINARY} crl
                          -in ${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca.crl
                          -text -noout
                  COMMAND ${OPENSSL_BINARY} crl
                          -in ${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca_jane_revoked.crl
                          -text -noout
                  COMMAND ${OPENSSL_BINARY} crl
                          -in ${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca_expired.crl
                          -text -noout
                  COMMAND ${OPENSSL_BINARY} crl
                          -in ${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca_not_yet_valid.crl
                          -text -noout
                  COMMAND ${OPENSSL_BINARY} crl
                          -in ${CMAKE_CURRENT_BINARY_DIR}/intermediate_ca_bad_signature.crl
                          -text -noout
                  DEPENDS Generate_kv_engine_X509_Test_CRL)
