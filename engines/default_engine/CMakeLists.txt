ADD_LIBRARY(default_engine SHARED assoc.c default_engine.c engine_manager.cc
            items.c slabs.c)

SET_TARGET_PROPERTIES(default_engine PROPERTIES PREFIX "")

IF (ENABLE_DTRACE)
  ADD_DEPENDENCIES(default_engine generate_memcached_dtrace_h)
  IF (DTRACE_NEED_INSTRUMENT)
      ADD_CUSTOM_COMMAND(TARGET default_engine PRE_LINK
                         COMMAND
                         ${DTRACE} -o de_dtrace.o
                                   -G
                                   -s ${Memcached_SOURCE_DIR}/memcached_dtrace.d
                                   *.o
                         WORKING_DIRECTORY ${Memcached_BINARY_DIR}/CMakeFiles/default_engine.dir/engines/default_engine)
      SET_TARGET_PROPERTIES(default_engine PROPERTIES LINK_FLAGS
        "${Memcached_BINARY_DIR}/CMakeFiles/default_engine.dir/engines/default_engine/de_dtrace.o")
  ENDIF (DTRACE_NEED_INSTRUMENT)
ENDIF (ENABLE_DTRACE)

TARGET_LINK_LIBRARIES(default_engine mcd_util platform ${COUCHBASE_NETWORK_LIBS})

INSTALL(TARGETS default_engine
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
