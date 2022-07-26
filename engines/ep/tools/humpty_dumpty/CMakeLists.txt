add_executable(humpty_dumpty
        humpty_dumpty.cc)
target_link_libraries(humpty_dumpty
    PRIVATE
        ep
        Folly::folly
        nlohmann_json::nlohmann_json)
add_sanitizers(humpty_dumpty)

# Relies on posix shell / redirection, so only run on *ix platforms.
if (UNIX)
    add_test(NAME humpty_dumpty_sanity
        COMMAND sh -c "($<TARGET_FILE:humpty_dumpty> ${CMAKE_CURRENT_SOURCE_DIR}/humpty_sanity_table.json 100 0 < ${CMAKE_CURRENT_SOURCE_DIR}/humpty_sanity_test.in > humpty_sanity_test.actual) &&
        diff -u ${CMAKE_CURRENT_SOURCE_DIR}/humpty_sanity_test.expected humpty_sanity_test.actual")
endif()
