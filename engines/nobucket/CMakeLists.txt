add_library(nobucket STATIC
            nobucket_public.h
            nobucket.cc)
kv_enable_pch(nobucket)
target_link_libraries(nobucket PRIVATE platform spdlog)
