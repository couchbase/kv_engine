ADD_LIBRARY(ewouldblock_engine SHARED ewouldblock_engine.cc)
TARGET_LINK_LIBRARIES(ewouldblock_engine
                      dirutils
                      engine_utilities
                      mcbp
                      mcd_util
                      xattr)
SET_TARGET_PROPERTIES(ewouldblock_engine PROPERTIES PREFIX "")
INSTALL(TARGETS ewouldblock_engine
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)
