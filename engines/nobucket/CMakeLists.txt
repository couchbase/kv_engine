ADD_LIBRARY(nobucket SHARED nobucket.c)
TARGET_LINK_LIBRARIES(nobucket platform)
SET_TARGET_PROPERTIES(nobucket PROPERTIES PREFIX "")

INSTALL(TARGETS nobucket
        RUNTIME DESTINATION lib/memcached
        LIBRARY DESTINATION lib/memcached
        ARCHIVE DESTINATION lib/memcached)
