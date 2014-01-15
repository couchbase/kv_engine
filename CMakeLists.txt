CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

#
# Set the include path
#

SET(INCLUDE_DIRS ${DEPS_INCLUDE_DIR}
                 ${LIBEVENT_INCLUDE_DIR}
                 ${SNAPPY_INCLUDE_DIR}
                 ${CMAKE_INSTALL_PREFIX}/include
                 ${CMAKE_CURRENT_SOURCE_DIR}/include
                 ${CMAKE_CURRENT_SOURCE_DIR}
                 ${CMAKE_CURRENT_SOURCE_DIR}/../platform/include
                 ${CMAKE_CURRENT_SOURCE_DIR}/../cbsasl/include)

IF (TCMALLOC_FOUND)
   SET(INCLUDE_DIRS ${INCLUDE_DIRS} ${TCMALLOC_INCLUDE_DIR})
ENDIF (TCMALLOC_FOUND)

INCLUDE_DIRECTORIES(BEFORE ${INCLUDE_DIRS})

IF (WIN32)
   INCLUDE_DIRECTORIES(AFTER ${CMAKE_CURRENT_SOURCE_DIR}/../platform/include/win32)
ELSE (WIN32)
   SET(MATH_LIBS "m")
   SET(THREAD_LIBS "pthread")
   SET(DLOPENLIB "dl")

   ADD_LIBRARY(file_logger SHARED extensions/loggers/file_logger.c)
   TARGET_LINK_LIBRARIES(file_logger platform z)
   SET_TARGET_PROPERTIES(file_logger PROPERTIES PREFIX "")
   SET_TARGET_PROPERTIES(file_logger PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)

   INSTALL(TARGETS file_logger
           RUNTIME DESTINATION bin
           LIBRARY DESTINATION lib/memcached
           ARCHIVE DESTINATION lib/memcached)

   ADD_EXECUTABLE(mcbasher programs/mcbasher.cc)
   TARGET_LINK_LIBRARIES(mcbasher platform ${COUCHBASE_NETWORK_LIBS})
   ADD_EXECUTABLE(timedrun programs/timedrun.c)
ENDIF (WIN32)

#
# Use TCMalloc if we have it
#
IF (TCMALLOC_FOUND)
   SET(TCMALLOC_LIBRARY ${TCMALLOC_LIBRARIES})
ELSE(TCMALLOC_FOUND)
   ADD_DEFINITIONS(-DDONT_HAVE_TCMALLOC=1)
ENDIF(TCMALLOC_FOUND)

#
# Add all of the libraries
#
ADD_LIBRARY(mcd_util SHARED
            utilities/config_parser.c
            utilities/engine_loader.c
            utilities/extension_loggers.c
            utilities/util.c)
ADD_LIBRARY(default_engine SHARED
            engines/default_engine/assoc.c
            engines/default_engine/default_engine.c
            engines/default_engine/items.c
            engines/default_engine/slabs.c)
ADD_LIBRARY(bucket_engine SHARED
            engines/bucket_engine/bucket_engine.c
            engines/bucket_engine/topkeys.c
            engines/bucket_engine/genhash.c)
ADD_LIBRARY(basic_engine_testsuite SHARED testsuite/basic_engine_testsuite.c)
ADD_LIBRARY(blackhole_logger SHARED extensions/loggers/blackhole_logger.c)
ADD_LIBRARY(fragment_rw_ops SHARED extensions/protocol/fragment_rw.c)
ADD_LIBRARY(stdin_term_handler SHARED extensions/daemon/stdin_check.c)
ADD_LIBRARY(tap_mock_engine SHARED engines/tap_mock_engine/tap_mock_engine.cc)
ADD_LIBRARY(bucket_engine_mock_engine SHARED
            engines/bucket_engine/mock_engine.c
            engines/bucket_engine/genhash.c)

SET_TARGET_PROPERTIES(default_engine PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(bucket_engine PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(basic_engine_testsuite PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(blackhole_logger PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(fragment_rw_ops PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(stdin_term_handler PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(tap_mock_engine PROPERTIES PREFIX "")
SET_TARGET_PROPERTIES(bucket_engine_mock_engine PROPERTIES PREFIX "")

#
# All of the binaries we're building
#
ADD_EXECUTABLE(bucket_engine_testapp
               engines/bucket_engine/testapp.c
               engines/bucket_engine/genhash.c)
ADD_EXECUTABLE(engine_testapp programs/engine_testapp.c programs/mock_server.c)
ADD_EXECUTABLE(cbsasladm programs/cbsasladm.c)
ADD_EXECUTABLE(mcstat programs/mcstat.c)
ADD_EXECUTABLE(memcached_sizes programs/sizes.c)
ADD_EXECUTABLE(memcached
               daemon/alloc_hooks.c
               daemon/cache.c
               daemon/daemon.c
               daemon/hash.c
               daemon/memcached.c
               daemon/privileges.c
               daemon/stats.c
               daemon/thread.c)

ADD_EXECUTABLE(memcached_testapp programs/testapp.c daemon/cache.c)
ADD_EXECUTABLE(gencode programs/gencode.cc)
SET_TARGET_PROPERTIES(gencode PROPERTIES COMPILE_FLAGS -I${CMAKE_CURRENT_SOURCE_DIR}/../libvbucket/include)

ADD_EXECUTABLE(genconfig programs/genconfig.cc)
SET_TARGET_PROPERTIES(genconfig PROPERTIES COMPILE_FLAGS -I${CMAKE_CURRENT_SOURCE_DIR}/../libvbucket/include)


#
# Add linker flags to all of the binaries
#
TARGET_LINK_LIBRARIES(bucket_engine mcd_util platform ${COUCHBASE_NETWORK_LIBS} ${MATH_LIBS})
TARGET_LINK_LIBRARIES(default_engine mcd_util platform ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(basic_engine_testsuite mcd_util platform ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(stdin_term_handler platform)
TARGET_LINK_LIBRARIES(fragment_rw_ops mcd_util platform ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(engine_testapp mcd_util platform ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(bucket_engine_testapp mcd_util platform ${COUCHBASE_NETWORK_LIBS} ${MATH_LIBS})
TARGET_LINK_LIBRARIES(cbsasladm platform ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(mcstat platform ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(tap_mock_engine platform ${COUCHBASE_NETWORK_LIBS})

TARGET_LINK_LIBRARIES(mcd_util platform)
TARGET_LINK_LIBRARIES(memcached mcd_util cbsasl platform ${SNAPPY_LIBRARIES} ${TCMALLOC_LIBRARY} ${LIBEVENT_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})
TARGET_LINK_LIBRARIES(memcached_testapp mcd_util platform ${SNAPPY_LIBRARIES} ${LIBEVENT_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})

TARGET_LINK_LIBRARIES(gencode cJSON platform)
TARGET_LINK_LIBRARIES(genconfig cJSON platform)


IF(WIN32)
    SET_TARGET_PROPERTIES(memcached PROPERTIES
                          LINK_FLAGS "/LIBPATH:${DEPS_LIB_DIR}")
    SET_TARGET_PROPERTIES(memcached_testapp PROPERTIES
                          LINK_FLAGS "/LIBPATH:${DEPS_LIB_DIR}")
ELSE(WIN32)
    SET_TARGET_PROPERTIES(memcached PROPERTIES
                          LINK_FLAGS "-L${DEPS_LIB_DIR}")
    SET_TARGET_PROPERTIES(memcached_testapp PROPERTIES
                          LINK_FLAGS "-L${DEPS_LIB_DIR}")
ENDIF(WIN32)

INSTALL (FILES include/memcached/allocator_hooks.h
               include/memcached/callback.h
               include/memcached/config_parser.h
               include/memcached/engine.h
               include/memcached/engine_common.h
               include/memcached/engine_testapp.h
               include/memcached/extension.h
               include/memcached/extension_loggers.h
               include/memcached/protocol_binary.h
               include/memcached/protocol_plugin.h
               include/memcached/server_api.h
               include/memcached/types.h
               include/memcached/upr.h
               include/memcached/util.h
               include/memcached/vbucket.h
               include/memcached/visibility.h
         DESTINATION include/memcached)

SET_TARGET_PROPERTIES(mcd_util PROPERTIES SOVERSION 1.0.0)
INSTALL(TARGETS mcd_util default_engine bucket_engine blackhole_logger fragment_rw_ops stdin_term_handler
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib/memcached
        ARCHIVE DESTINATION lib/memcached)

SET_TARGET_PROPERTIES(mcd_util PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)
SET_TARGET_PROPERTIES(default_engine PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)
SET_TARGET_PROPERTIES(bucket_engine PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)
SET_TARGET_PROPERTIES(blackhole_logger PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)
SET_TARGET_PROPERTIES(fragment_rw_ops PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)
SET_TARGET_PROPERTIES(stdin_term_handler PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib/memcached)


INSTALL(TARGETS engine_testapp cbsasladm mcstat memcached gencode genconfig
        RUNTIME DESTINATION bin)

ADD_TEST(memcached-sizes memcached_sizes)
ADD_TEST(memcached-basic-unit-tests memcached_testapp)
ADD_TEST(bucket_engine-basic-unit-tests bucket_engine_testapp)
ADD_TEST(basic-engine-tests engine_testapp -E default_engine.so -T basic_engine_testsuite.so)

IF(${COUCHBASE_PYTHON})
ADD_CUSTOM_COMMAND(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/generated_breakdancer_testsuite.c
                  COMMAND
                        ${PYTHON_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/testsuite/breakdancer/engine_test.py > ${CMAKE_CURRENT_BINARY_DIR}/generated_breakdancer_testsuite.c
                  DEPENDS
                        testsuite/breakdancer/breakdancer.py
                        testsuite/breakdancer/engine_test.py
                  COMMENT "Generating the test suite")

ADD_LIBRARY(breakdancer_testsuite SHARED
            testsuite/breakdancer/suite_stubs.c
            ${CMAKE_CURRENT_BINARY_DIR}/generated_breakdancer_testsuite.c)
SET_TARGET_PROPERTIES(breakdancer_testsuite PROPERTIES PREFIX "")
ADD_TEST(breakdancer-engine-tests engine_testapp -E default_engine.so -T breakdancer_testsuite.so)
ENDIF(${COUCHBASE_PYTHON})
