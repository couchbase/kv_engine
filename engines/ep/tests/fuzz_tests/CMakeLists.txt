add_compile_options_disable_optimization()

target_sources(ep-engine_ep_unit_tests PUBLIC
    active_stream_fuzz_test.cc
    dockey_fuzz_test.cc
    fuzz_test_helpers.cc
)
