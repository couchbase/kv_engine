ADD_EXECUTABLE(mctimings mctimings.cc)
TARGET_LINK_LIBRARIES(mctimings
                      mcutils
                      mc_client_connection
                      cJSON
                      platform)
INSTALL(TARGETS mctimings RUNTIME DESTINATION bin)
