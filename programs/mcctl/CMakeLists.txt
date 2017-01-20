ADD_EXECUTABLE(mcctl mcctl.cc)
TARGET_LINK_LIBRARIES(mcctl
                      mc_client_connection
                      mcd_util
                      mcutils
                      platform)
INSTALL(TARGETS mcctl RUNTIME DESTINATION bin)
