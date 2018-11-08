ADD_EXECUTABLE(mcstat mcstat.cc)
TARGET_LINK_LIBRARIES(mcstat
                      platform
                      mcutils
                      mc_client_connection
                      getpass)
add_sanitizers(mcstat)
INSTALL(TARGETS mcstat RUNTIME DESTINATION bin)
