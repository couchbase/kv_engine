/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include <fmt/format.h>
#include <folly/portability/Unistd.h>
#include <logger/logger.h>
#include <mcbp/codec/frameinfo.h>
#include <memcached/stat_group.h>
#include <platform/dirutils.h>
#include <platform/terminal_color.h>
#include <platform/timeutils.h>
#include <programs/mc_program_getopt.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_commands.h>
#include <snapshot/snapshot_downloader.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <cstdio>
#include <iostream>

using namespace cb::terminal;

using namespace std::string_view_literals;

// arg spdlog::level::from_str didn't like these strings..
static spdlog::level::level_enum to_level(std::string_view view) {
    if (view == "trace"sv) {
        return spdlog::level::trace;
    }
    if (view == "debug"sv) {
        return spdlog::level::debug;
    }
    if (view == "info"sv) {
        return spdlog::level::info;
    }
    if (view == "warning"sv) {
        return spdlog::level::warn;
    }
    if (view == "error"sv) {
        return spdlog::level::err;
    }
    if (view == "critical"sv) {
        return spdlog::level::critical;
    }

    throw std::runtime_error(fmt::format("Unknown log level: \"{}\"", view));
}

std::shared_ptr<spdlog::logger> logger;

static void usage(McProgramGetopt& instance, int exitcode) {
    std::cerr << R"(Usage: mcvbcopy [options]

Options:

)" << instance << std::endl
              << std::endl;
    std::exit(exitcode);
}

static std::filesystem::path getManifestFile(Vbid vb) {
    return fmt::format("mcvbpcopy.{}.manifest", vb.get());
}

static nlohmann::json getVbucketManifest(MemcachedConnection& connection,
                                         Vbid vbucket) {
    auto path = getManifestFile(vbucket);
    if (exists(path)) {
        try {
            nlohmann::json json = nlohmann::json::parse(cb::io::loadFile(path));
            logger->info("Using cached manifest from {}: {}",
                         path.string(),
                         json.dump());
            return json;
        } catch (const std::exception&) {
            remove(path);
        }
    }

    // open new snapshot and save to disk
    nlohmann::json json;
    try {
        logger->info("Create manifest on the server");
        BinprotGenericCommand cmd(cb::mcbp::ClientOpcode::PrepareSnapshot);
        cmd.setVBucket(vbucket);
        auto rsp = connection.execute(cmd);
        if (!rsp.isSuccess()) {
            throw std::runtime_error(
                    fmt::format("Failed to open VBucket snapshot {}: {}",
                                rsp.getStatus(),
                                rsp.getDataView()));
        }
        json = nlohmann::json::parse(rsp.getDataView());

        FILE* fp = fopen(path.string().c_str(), "a");
        if (!fp) {
            throw std::system_error(
                    std::make_error_code(static_cast<std::errc>(errno)),
                    "Failed to open file");
        }
        fprintf(fp, "%s\n", json.dump().c_str());
        fflush(fp);
        fclose(fp);
    } catch (const std::exception&) {
        throw;
    }
    logger->info("vbucket manifest returned from the server: {}", json.dump());
    return json;
}

int main(int argc, char** argv) {
    auto log_level = spdlog::level::info;

    McProgramGetopt getopt;
    using cb::getopt::Argument;

    std::optional<Vbid> vbid;
    std::string bucket;
    std::size_t fsync_size = 50 * 1024 * 1024;

    getopt.addOption({[&log_level](auto value) {
                          try {
                              log_level = to_level(value);
                          } catch (const std::exception& exception) {
                              fmt::println(stderr,
                                           "Unknown log level {}: {}",
                                           value,
                                           exception.what());
                              std::exit(EXIT_FAILURE);
                          }
                      },
                      "log-level",
                      Argument::Required,
                      "level",
                      "The log level (trace, debug, info ,warning, error, "
                      "critical). Default: info"});

    getopt.addOption(
            {[&vbid](auto value) { vbid = Vbid(stoi(std::string{value})); },
             "vbucket",
             Argument::Required,
             "vbid",
             "The vbucket to fetch"});

    getopt.addOption({[&bucket](auto value) { bucket = std::string{value}; },
                      "bucket",
                      Argument::Required,
                      "name",
                      "The bucket to operate on"});

    getopt.addOption({[&fsync_size](auto value) {
                          try {
                              fsync_size = std::stoull(std::string{value});
                          } catch (const std::exception&) {
                              std::cerr << "--fsync-size must be a number"
                                        << std::endl;
                              std::exit(EXIT_FAILURE);
                          }
                      },
                      "fsync-size",
                      Argument::Required,
                      "size",
                      "The size (in bytes) to fsync()"});

    getopt.addOption({[&getopt](auto) {
                          usage(getopt, EXIT_SUCCESS);
                          std::exit(EXIT_SUCCESS);
                      },
                      "help",
                      "This help text"});

    auto arguments = getopt.parse(
            argc, argv, [&getopt]() { usage(getopt, EXIT_FAILURE); });

    if (!vbid.has_value()) {
        std::cerr << "VBucket must be specified" << std::endl;
    }

    logger = std::make_shared<spdlog::logger>(
            "mcvbcppy_logger",
            std::make_shared<spdlog::sinks::stderr_color_sink_st>());
    logger->set_level(log_level);
    logger->set_pattern("%^%Y-%m-%dT%T.%f%z %l %v%$");
    logger->flush_on(log_level);
    try {
        getopt.assemble();

        auto connection = getopt.getConnection();
        connection->setAgentName("mcvbcopy/" PRODUCT_VERSION);
        connection->setFeatures(
                {cb::mcbp::Feature::XERROR, cb::mcbp::Feature::JSON});
        connection->selectBucket(bucket);

        auto download = getopt.getConnection();
        download->setAgentName("mcvbcopy/" PRODUCT_VERSION);
        download->setFeatures(
                {cb::mcbp::Feature::XERROR, cb::mcbp::Feature::JSON});
        download->selectBucket(bucket);

        // check for manifest and fetch if we don't have it
        nlohmann::json manifest = getVbucketManifest(*connection, *vbid);
        const auto uuid = manifest["uuid"].get<std::string>();

        snapshot::download(std::move(download),
                           std::filesystem::current_path(),
                           manifest,
                           [](auto level, auto msg, auto json) {
                               cb::logger::logWithContext(
                                       *logger, level, msg, std::move(json));
                           });

        logger->info("All files successfully downloaded");
        logger->info("Close snapshot");
        logger->debug("Remove cached file");
        remove(getManifestFile(*vbid));
        // close manifest
        logger->debug("Close on server");
        connection->execute(BinprotGenericCommand(
                cb::mcbp::ClientOpcode::ReleaseSnapshot, uuid));
    } catch (const std::exception& ex) {
        logger->critical("Failure: {}", ex.what());
        logger->flush();
        logger.reset();
        return EXIT_FAILURE;
    }
    logger->flush();
    logger.reset();
    return EXIT_SUCCESS;
}
