add_library(executor STATIC
            executor.cc
            executor.h)
target_link_libraries(executor PUBLIC Folly::folly)
