ADD_LIBRARY(tap_mock_engine SHARED tap_mock_engine.cc)
SET_TARGET_PROPERTIES(tap_mock_engine PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(tap_mock_engine platform ${COUCHBASE_NETWORK_LIBS})
