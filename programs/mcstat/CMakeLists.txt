add_executable(mcstat mcstat.cc tasks_table_printer.cc stat_formatters.cc)
kv_enable_pch(mcstat)
target_link_libraries(mcstat PRIVATE mc_client_connection mc_program_utils platform)
add_sanitizers(mcstat)
install(TARGETS mcstat RUNTIME DESTINATION bin)
AddToStandalonePackage(TARGETS mcstat PACKAGES admin_tools)

cb_add_test_executable(tasks_table_printer_test tasks_table_printer_test.cc tasks_table_printer.cc)
kv_enable_pch(tasks_table_printer_test)
target_link_libraries(tasks_table_printer_test PRIVATE platform GTest::gtest GTest::gtest_main)
add_sanitizers(tasks_table_printer_test)

cb_add_test_executable(stat_formatters_test stat_formatters_test.cc stat_formatters.cc tasks_table_printer.cc)
kv_enable_pch(stat_formatters_test)
target_link_libraries(stat_formatters_test PRIVATE platform GTest::gtest GTest::gtest_main)
add_sanitizers(stat_formatters_test)

add_test(NAME tasks_table_printer_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND tasks_table_printer_test)
add_test(NAME stat_formatters_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND stat_formatters_test)
add_test(NAME mcstat_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mcstat --help)
add_test(NAME mcstat_statkey_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mcstat --help=statkey)
add_test(NAME mcstat_statkey_thread_usage_test
        WORKING_DIRECTORY ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}
        COMMAND mcstat --help=threads)
