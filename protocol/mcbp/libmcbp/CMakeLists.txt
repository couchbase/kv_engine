add_library(mcbp STATIC
            ${Memcached_SOURCE_DIR}/include/libmcbp/mcbp.h
            dump.cc
            )
target_link_libraries(mcbp mcd_util)