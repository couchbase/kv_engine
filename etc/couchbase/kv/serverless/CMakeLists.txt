add_executable(generate_serverless_config generate_serverless_config.cc)
target_link_libraries(generate_serverless_config mcd_util nlohmann_json::nlohmann_json)
add_custom_command(OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/configuration.json
                   COMMAND generate_serverless_config
                   DEPENDS generate_serverless_config
                   COMMENT "Generating serverless config")

add_custom_target(generate_serverless_configuration ALL
                  DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/configuration.json)

install(FILES README.md ${CMAKE_CURRENT_BINARY_DIR}/configuration.json
        DESTINATION etc/couchbase/kv/serverless)
