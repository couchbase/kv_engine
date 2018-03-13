ADD_LIBRARY(mcd_tracing SHARED tracer.h tracer.cc)
TARGET_LINK_LIBRARIES(mcd_tracing engine_utilities platform)
SET_TARGET_PROPERTIES(mcd_tracing PROPERTIES SOVERSION 1.0.0)
add_sanitizers(mcd_tracing)

INSTALL(TARGETS mcd_tracing RUNTIME DESTINATION bin LIBRARY DESTINATION lib
                ARCHIVE DESTINATION lib)
