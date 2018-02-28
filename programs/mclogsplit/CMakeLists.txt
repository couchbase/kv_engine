add_executable(mclogsplit mclogsplit.cc)
target_link_libraries(mclogsplit dirutils mcd_util)
install(TARGETS mclogsplit RUNTIME DESTINATION bin)
