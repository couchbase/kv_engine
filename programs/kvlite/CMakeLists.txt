cb_add_test_executable(kvlite kvlite.cc)
kv_enable_pch(kvlite)
target_link_libraries(kvlite PRIVATE cluster_framework)
