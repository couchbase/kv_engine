ADD_LIBRARY(blackhole_logger SHARED blackhole_logger.cc)
SET_TARGET_PROPERTIES(blackhole_logger PROPERTIES PREFIX "")

ADD_LIBRARY(file_logger SHARED file_logger.cc
            file_logger_utilities.cc
            file_logger_utilities.h)
SET_TARGET_PROPERTIES(file_logger PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(file_logger mcd_time platform dirutils)

INSTALL(TARGETS file_logger
        RUNTIME DESTINATION lib/memcached
        LIBRARY DESTINATION lib/memcached
        ARCHIVE DESTINATION lib/memcached)
