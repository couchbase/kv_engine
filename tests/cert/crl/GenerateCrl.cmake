# Generate a CRL file using an OpenSSL CA configuration.
#
# Required parameters (passed via -D):
#   OPENSSL        - path to the openssl binary
#   CA_CNF         - path to the OpenSSL CA config file
#   DB_DIR         - directory used for the CA database files
#   OUTPUT         - destination path for the generated CRL file
#
# Optional parameters:
#   REVOKED_SERIAL - hex serial number of a certificate to mark revoked
#                    (e.g. "01").  All certs currently use -set_serial 1.
#   REVOKED_DATE   - revocation timestamp in YYMMDDHHMMSSZ format
#                    (defaults to "000101000000Z" when not provided)
#   CRL_DAYS       - validity period in days (passed as -crldays to openssl ca).
#                    Use 0 to produce an immediately-expired CRL for testing.

cmake_minimum_required(VERSION 3.13)

file(MAKE_DIRECTORY ${DB_DIR})

# Initialise the CA database every time so the build is reproducible.
file(WRITE ${DB_DIR}/index.txt "")
file(WRITE ${DB_DIR}/serial "01\n")
file(WRITE ${DB_DIR}/crlnumber "01\n")

if (DEFINED REVOKED_SERIAL)
    if (NOT DEFINED REVOKED_DATE)
        set(REVOKED_DATE "000101000000Z")
    endif ()
    # index.txt row: R <cert-expiry> <revoke-date> <serial> unknown <subject>
    # A far-future expiry (2049) prevents the entry from being pruned.
    file(WRITE ${DB_DIR}/index.txt
         "R\t491231235959Z\t${REVOKED_DATE}\t${REVOKED_SERIAL}\tunknown\t/CN=revoked\n")
endif ()

if (DEFINED CRL_DAYS)
    if (CRL_DAYS EQUAL 0)
        # OpenSSL 3 rejects 0 as non-positive for -crldays.  Use -crlsec 1 so
        # the CRL expires one second after generation, which is effectively
        # immediately expired by the time any test runner accesses the file.
        set(_crl_days_arg "-crlsec" "1")
    else ()
        set(_crl_days_arg "-crldays" ${CRL_DAYS})
    endif ()
else ()
    set(_crl_days_arg "")
endif ()

execute_process(
    COMMAND ${OPENSSL} ca
            -gencrl
            ${_crl_days_arg}
            -config ${CA_CNF}
            -out ${OUTPUT}.tmp
    RESULT_VARIABLE result
    OUTPUT_VARIABLE stdout
    ERROR_VARIABLE  stderr
)
if (NOT result EQUAL 0)
    message(STATUS "openssl stdout: ${stdout}")
    message(STATUS "openssl stderr: ${stderr}")
    message(FATAL_ERROR "Failed to generate CRL (exit code: ${result})")
endif ()

file(RENAME ${OUTPUT}.tmp ${OUTPUT})
