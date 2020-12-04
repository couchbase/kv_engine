add_executable(kvlite kvlite.cc)
kv_enable_pch(kvlite)
target_link_libraries(kvlite cluster_framework)
