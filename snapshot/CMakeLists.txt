add_library(snapshot STATIC
        file_downloader.cc
        file_downloader.h
        snapshot_downloader.cc
        snapshot_downloader.h
)
kv_enable_pch(snapshot)
target_link_libraries(snapshot PUBLIC mc_client_connection platform spdlog::spdlog)
add_sanitizers(snapshot)
