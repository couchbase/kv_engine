list(APPEND CBSASL_SOURCES
     ${Memcached_SOURCE_DIR}/include/cbsasl/cbsasl.h
     ${Memcached_SOURCE_DIR}/include/cbsasl/saslauthd_config.h
     ${Memcached_SOURCE_DIR}/cbsasl/cbsasl_internal.h
     ${Memcached_SOURCE_DIR}/cbsasl/client.cc
     ${Memcached_SOURCE_DIR}/cbsasl/common.cc
     ${Memcached_SOURCE_DIR}/cbsasl/log.cc
     ${Memcached_SOURCE_DIR}/cbsasl/mechanismfactory.cc
     ${Memcached_SOURCE_DIR}/cbsasl/mechanismfactory.h
     ${Memcached_SOURCE_DIR}/cbsasl/plain/check_password.cc
     ${Memcached_SOURCE_DIR}/cbsasl/plain/check_password.h
     ${Memcached_SOURCE_DIR}/cbsasl/plain/plain.cc
     ${Memcached_SOURCE_DIR}/cbsasl/plain/plain.h
     ${Memcached_SOURCE_DIR}/cbsasl/password_database.cc
     ${Memcached_SOURCE_DIR}/cbsasl/password_database.h
     ${Memcached_SOURCE_DIR}/cbsasl/pwconv.cc
     ${Memcached_SOURCE_DIR}/cbsasl/pwconv.h
     ${Memcached_SOURCE_DIR}/cbsasl/pwfile.cc
     ${Memcached_SOURCE_DIR}/cbsasl/pwfile.h
     ${Memcached_SOURCE_DIR}/cbsasl/saslauthd_config.cc
     ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/scram-sha.cc
     ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/scram-sha.h
     ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/stringutils.cc
     ${Memcached_SOURCE_DIR}/cbsasl/scram-sha/stringutils.h
     ${Memcached_SOURCE_DIR}/cbsasl/server.cc
     ${Memcached_SOURCE_DIR}/cbsasl/strcmp.cc
     ${Memcached_SOURCE_DIR}/cbsasl/strerror.cc
     ${Memcached_SOURCE_DIR}/cbsasl/user.cc
     ${Memcached_SOURCE_DIR}/cbsasl/user.h
     ${Memcached_SOURCE_DIR}/cbsasl/util.h)

if (NOT WIN32)
    list(APPEND CBSASL_SOURCES
         ${Memcached_SOURCE_DIR}/cbsasl/plain/saslauthd.cc
         ${Memcached_SOURCE_DIR}/cbsasl/plain/saslauthd.h)
endif (NOT WIN32)

add_library(cbsasl SHARED ${CBSASL_SOURCES})
generate_export_header(cbsasl
                       EXPORT_MACRO_NAME CBSASL_PUBLIC_API
                       EXPORT_FILE_NAME ${PROJECT_BINARY_DIR}/include/cbsasl/visibility.h)
set_target_properties(cbsasl PROPERTIES SOVERSION 1.1.1)
target_link_libraries(cbsasl cbcrypto dirutils cJSON platform)

install(TARGETS cbsasl
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
