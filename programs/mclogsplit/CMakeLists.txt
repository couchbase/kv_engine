add_executable(mclogsplit mclogsplit.cc)
target_link_libraries(mclogsplit dirutils)
install(TARGETS mclogsplit RUNTIME DESTINATION bin)
