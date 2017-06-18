add_executable(mctrace mctrace.cc)
target_link_libraries(mctrace
                      getpass
                      mc_client_connection
                      mcd_util
                      mcutils
                      platform)
install(TARGETS mctrace RUNTIME DESTINATION bin)
