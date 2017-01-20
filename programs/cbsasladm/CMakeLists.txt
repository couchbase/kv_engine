ADD_EXECUTABLE(cbsasladm
               cbsasladm.cc
               ${Memcached_SOURCE_DIR}/cbsasl/log.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.cc
               ${Memcached_SOURCE_DIR}/cbsasl/pwconv.h
               ${Memcached_SOURCE_DIR}/cbsasl/user.cc
               ${Memcached_SOURCE_DIR}/cbsasl/user.h)
TARGET_LINK_LIBRARIES(cbsasladm
                      cbsasl
                      mc_client_connection
                      mcd_util
                      mcutils
                      platform)
INSTALL(TARGETS cbsasladm RUNTIME DESTINATION bin)
