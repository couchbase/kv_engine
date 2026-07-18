# mock_stat_collector.h is not compiled into any real CMake target; it is a
# test-only mock header pulled in directly by test .cc files belonging to
# several unrelated targets (daemon, ep-engine, memcached_statistics_test,
# ...). Without a compile_commands.json entry of its own, tools relying on
# that database (clang-tidy, clangd) have to guess its compile flags from the
# nearest compiled file in the source tree, which can land on
# statistics/gen_stat_definitions.cc (the genstats code generator, which does
# not have the include path to the generated stats header). That produces
# bogus "generated_stats.h file not found" errors (MB-65361). Compile it into
# an unused OBJECT library, linked against statistics, purely so it gets a
# valid compile_commands.json entry.
add_library(statistics_mock_stat_collector OBJECT EXCLUDE_FROM_ALL
        mock_stat_collector.cc)
kv_enable_pch(statistics_mock_stat_collector)
target_link_libraries(statistics_mock_stat_collector PRIVATE
        statistics
        GTest::gmock)
