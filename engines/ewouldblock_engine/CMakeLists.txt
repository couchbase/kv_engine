ADD_LIBRARY(ewouldblock_engine SHARED ewouldblock_engine.cc)
TARGET_LINK_LIBRARIES(ewouldblock_engine
                      dirutils
                      engine_utilities
                      mcd_util
                      xattr)
SET_TARGET_PROPERTIES(ewouldblock_engine PROPERTIES PREFIX "")
