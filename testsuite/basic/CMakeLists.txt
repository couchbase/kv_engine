ADD_LIBRARY(basic_engine_testsuite SHARED basic_engine_testsuite.cc)
SET_TARGET_PROPERTIES(basic_engine_testsuite PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(basic_engine_testsuite mcd_util platform ${COUCHBASE_NETWORK_LIBS})
