ADD_LIBRARY(ewouldblock_engine SHARED ewouldblock_engine.cc)
TARGET_LINK_LIBRARIES(ewouldblock_engine mcd_util dirutils)
SET_TARGET_PROPERTIES(ewouldblock_engine PROPERTIES PREFIX "")
