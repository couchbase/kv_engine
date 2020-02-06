add_library(ewouldblock_engine STATIC
            ewouldblock_engine.h
            ewouldblock_engine_public.h
            ewouldblock_engine.cc)
target_link_libraries(ewouldblock_engine
                      dirutils
                      engine_utilities
                      mcbp
                      mcd_util
                      xattr)
