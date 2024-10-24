add_executable(mchistogram mchistogram.cc)
kv_enable_pch(mchistogram)
target_link_libraries(mchistogram
        PRIVATE
        fmt::fmt
        mcd_util
        nlohmann_json::nlohmann_json
        platform)
add_sanitizers(mchistogram)
install(TARGETS mchistogram RUNTIME DESTINATION bin)
AddToStandalonePackage(TARGETS mchistogram PACKAGES admin_tools)
