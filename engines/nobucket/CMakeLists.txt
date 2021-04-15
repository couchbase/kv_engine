add_library(nobucket STATIC
            nobucket_public.h
            nobucket.cc)
target_link_libraries(nobucket platform)
