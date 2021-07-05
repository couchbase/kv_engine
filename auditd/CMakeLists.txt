include_directories(BEFORE ${Platform_SOURCE_DIR}/include
                           ${Memcached_BINARY_DIR}/auditd)

add_subdirectory(generator)
add_subdirectory(src)
add_subdirectory(tests)
