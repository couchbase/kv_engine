add_library(mcd_fuzzing STATIC
    init.h
    init.cc)
set_property(TARGET mcd_fuzzing PROPERTY POSITION_INDEPENDENT_CODE 1)
target_link_libraries(mcd_fuzzing PRIVATE
    fmt::fmt
    nlohmann_json::nlohmann_json
    Folly::headers
    GTest::gtest)
if(HAVE_FUZZTEST)
    cb_link_fuzztest(mcd_fuzzing)
endif()
add_sanitizers(mcd_fuzzing)
kv_enable_pch(mcd_fuzzing)
