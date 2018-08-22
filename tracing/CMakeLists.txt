ADD_LIBRARY(mcd_tracing
  STATIC tracer.h tracer.cc trace_helpers.h)
set_property(TARGET mcd_tracing PROPERTY POSITION_INDEPENDENT_CODE 1)
TARGET_LINK_LIBRARIES(mcd_tracing engine_utilities platform)
add_sanitizers(mcd_tracing)
