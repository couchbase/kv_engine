# CorruptCrl.cmake - Produce a PEM CRL whose signature bytes are corrupted.
# Used to exercise X509_V_ERR_CRL_SIGNATURE_FAILURE handling in policy tests.
#
# Required parameters (passed via -D):
#   OPENSSL  - path to the openssl binary
#   PYTHON3  - path to the python3 interpreter
#   INPUT    - path to a valid PEM CRL file
#   OUTPUT   - destination path for the corrupted CRL file

cmake_minimum_required(VERSION 3.13)

# Step 1: PEM → DER
execute_process(
    COMMAND ${OPENSSL} crl -in ${INPUT} -outform DER -out ${OUTPUT}.der
    RESULT_VARIABLE result
    OUTPUT_VARIABLE stdout
    ERROR_VARIABLE  stderr
)
if (NOT result EQUAL 0)
    message(STATUS "openssl stdout: ${stdout}")
    message(STATUS "openssl stderr: ${stderr}")
    message(FATAL_ERROR "Failed to convert CRL to DER (exit code: ${result})")
endif ()

# Step 2: flip 16 bytes in the middle of the RSA signature (last 32..16 bytes
# of the DER).  Writing the script to disk avoids shell-quoting pitfalls.
file(WRITE ${OUTPUT}.corrupt.py
"import sys\n"
"data = bytearray(open(sys.argv[1], 'rb').read())\n"
"for i in range(-32, -16):\n"
"    data[i] ^= 0xFF\n"
"open(sys.argv[2], 'wb').write(bytes(data))\n")

execute_process(
    COMMAND ${PYTHON3} ${OUTPUT}.corrupt.py ${OUTPUT}.der ${OUTPUT}.der.corrupt
    RESULT_VARIABLE result
    OUTPUT_VARIABLE stdout
    ERROR_VARIABLE  stderr
)
if (NOT result EQUAL 0)
    message(STATUS "python3 stdout: ${stdout}")
    message(STATUS "python3 stderr: ${stderr}")
    message(FATAL_ERROR "Failed to corrupt CRL DER (exit code: ${result})")
endif ()

# Step 3: corrupted DER → PEM  (openssl crl in conversion mode does not verify
# the signature, so this succeeds even with the damaged bytes)
execute_process(
    COMMAND ${OPENSSL} crl
            -in ${OUTPUT}.der.corrupt -inform DER
            -out ${OUTPUT}.tmp
    RESULT_VARIABLE result
    OUTPUT_VARIABLE stdout
    ERROR_VARIABLE  stderr
)
if (NOT result EQUAL 0)
    message(STATUS "openssl stdout: ${stdout}")
    message(STATUS "openssl stderr: ${stderr}")
    message(FATAL_ERROR "Failed to convert corrupted DER to PEM (exit code: ${result})")
endif ()

file(RENAME ${OUTPUT}.tmp ${OUTPUT})
