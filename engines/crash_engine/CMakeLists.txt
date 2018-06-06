ADD_LIBRARY(crash_engine SHARED crash_engine.cc)
SET_TARGET_PROPERTIES(crash_engine PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(crash_engine engine_utilities platform)
INSTALL(TARGETS crash_engine
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
