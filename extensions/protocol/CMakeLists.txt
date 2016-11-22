ADD_LIBRARY(testapp_extension SHARED testapp_extension.cc)
SET_TARGET_PROPERTIES(testapp_extension PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(testapp_extension mcd_util platform ${COUCHBASE_NETWORK_LIBS})
