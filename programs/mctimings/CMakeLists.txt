ADD_EXECUTABLE(mctimings mctimings.cc)
TARGET_LINK_LIBRARIES(mctimings
                      mcutils
                      mc_client_connection
                      getpass
                      platform)
add_sanitizers(mctimings)
INSTALL(TARGETS mctimings RUNTIME DESTINATION bin)
