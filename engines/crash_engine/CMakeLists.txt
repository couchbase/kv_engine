add_library(crash_engine STATIC crash_engine.cc)
kv_enable_pch(crash_engine)
target_link_libraries(crash_engine PRIVATE engine_utilities platform)
