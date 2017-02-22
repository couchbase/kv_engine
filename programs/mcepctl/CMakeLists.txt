ADD_EXECUTABLE(mcepctl mcepctl.cc)
TARGET_LINK_LIBRARIES(mcepctl
                      mc_client_connection
                      mcd_util
                      mcutils
                      platform)
INSTALL(TARGETS mcepctl RUNTIME DESTINATION bin)
