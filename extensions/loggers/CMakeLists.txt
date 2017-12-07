ADD_LIBRARY(blackhole_logger SHARED blackhole_logger.cc)
SET_TARGET_PROPERTIES(blackhole_logger PROPERTIES PREFIX "")

ADD_LIBRARY(file_logger SHARED file_logger.cc
            file_logger_utilities.cc
            file_logger_utilities.h)
SET_TARGET_PROPERTIES(file_logger PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(file_logger mcd_time platform dirutils)

ADD_LIBRARY(spdlogger SHARED
            spdlogger.cc
            custom_rotating_file_sink.cc
            custom_rotating_file_sink.h
            file_logger_utilities.cc
            file_logger_utilities.h)

SET_TARGET_PROPERTIES(spdlogger PROPERTIES PREFIX "")
TARGET_LINK_LIBRARIES(spdlogger platform dirutils spdlog)
set_target_properties(spdlogger PROPERTIES SOVERSION 1.0.0)

INSTALL(TARGETS file_logger spdlogger
        RUNTIME DESTINATION lib/memcached
        LIBRARY DESTINATION lib/memcached
        ARCHIVE DESTINATION lib/memcached)
