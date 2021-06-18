add_library(ewouldblock_engine STATIC
            ewouldblock_engine.h
            ewouldblock_engine_public.h
            ewouldblock_engine.cc)
kv_enable_pch(ewouldblock_engine)
target_link_libraries(ewouldblock_engine PRIVATE
                      engine_utilities
                      mcbp
                      mcd_util
                      platform
                      spdlog
                      xattr)
