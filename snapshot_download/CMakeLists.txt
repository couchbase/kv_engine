add_library(snapshot_download STATIC
        file_downloader.cc
        file_downloader.h
        snapshot_downloader.cc
        snapshot_downloader.h
)
kv_enable_pch(snapshot_download)
target_link_libraries(snapshot_download PUBLIC mc_client_connection platform spdlog::spdlog)
add_sanitizers(snapshot_download)
