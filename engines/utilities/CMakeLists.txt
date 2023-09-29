add_library(engine_utilities STATIC
        engine.cc
        engine_error.cc)
set_property(TARGET engine_utilities PROPERTY POSITION_INDEPENDENT_CODE 1)
kv_enable_pch(engine_utilities)
cb_enable_unity_build(engine_utilities)
target_link_libraries(engine_utilities PRIVATE platform)
