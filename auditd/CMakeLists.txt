INCLUDE_DIRECTORIES(BEFORE ${Platform_SOURCE_DIR}/include
                           ${Memcached_BINARY_DIR}/auditd)

ADD_SUBDIRECTORY(generator)
ADD_SUBDIRECTORY(src)
if (COUCHBASE_KV_BUILD_UNIT_TESTS)
    ADD_SUBDIRECTORY(tests)
endif ()

