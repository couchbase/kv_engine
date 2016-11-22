ADD_LIBRARY(stdin_term_handler SHARED stdin_check.cc)
SET_TARGET_PROPERTIES(stdin_term_handler PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(stdin_term_handler platform)
INSTALL(TARGETS stdin_term_handler
        RUNTIME DESTINATION lib/memcached
        LIBRARY DESTINATION lib/memcached
        ARCHIVE DESTINATION lib/memcached)
