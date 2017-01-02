ADD_LIBRARY(default_engine SHARED
            assoc.cc
            assoc.h
            default_engine.cc
            default_engine_internal.h
            engine_manager.cc
            engine_manager.h
            items.cc
            items.h
            scrubber_task.cc
            scrubber_task.h
            slabs.cc
            slabs.h)

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
                         WORKING_DIRECTORY ${Memcached_BINARY_DIR}/engines/default_engine/CMakeFiles/default_engine.dir)
      SET_TARGET_PROPERTIES(default_engine PROPERTIES LINK_FLAGS
        "${Memcached_BINARY_DIR}/engines/default_engine/CMakeFiles/default_engine.dir/de_dtrace.o")
  ENDIF (DTRACE_NEED_INSTRUMENT)
ENDIF (ENABLE_DTRACE)

TARGET_LINK_LIBRARIES(default_engine engine_utilities mcd_util platform ${COUCHBASE_NETWORK_LIBS})

INSTALL(TARGETS default_engine
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
