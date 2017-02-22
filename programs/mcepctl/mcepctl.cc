/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

/*
 * mcepctl - Utility program to control ep-engine (converted from Python to
 *           reuse all of the SSL / SCRAM / IPv6 goodness)
 */

#include "config.h"

#include <chrono>
#include <getopt.h>
#include <memcached/openssl.h>
#include <memcached/protocol_binary.h>
#include <memcached/util.h>
#include <platform/cb_malloc.h>
#include <platform/platform.h>
#include <programs/hostname_utils.h>
#include <protocol/connection/client_connection.h>
#include <protocol/connection/client_mcbp_connection.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <thread>
#include <utilities/protocol2text.h>

static void usage() {
    std::cerr << "Usage: mcepctl [options] command arguments" << std::endl
              << std::endl
              << "Options: " << std::endl
              << "\t-h host[:port]  Hostname with optional port (default localhost)"
              << std::endl
              << "\t-p port         Port number to connect to (default 11210)"
              << std::endl
              << "\t-4              Force use of IPv4" << std::endl
              << "\t-6              Force use of IPv6" << std::endl
              << "\t-b bucket       Connect to the specified bucket"
              << std::endl
              << "\t-u username     Authenticate using the specified username"
              << std::endl
              << "\t-P password     Authenticate using the specified password"
              << std::endl
              << "\t-s              Connect over SSL" << std::endl
              << std::endl
              << "Commands:" << std::endl
              << "\tstop              - Stop persistence" << std::endl
              << "\tstart             - Start persistence" << std::endl
              << "\tdrain             - wait until queues are drained"
              << std::endl
              << "\tset               - set various parameters (see below)"
              << std::endl
              << "\tset_vbucket_param - set vbucket parameters (see below)"
              << std::endl
              << std::endl

              << "  Available params for set checkpoint_param:" << std::endl
              << "    chk_max_items                - Max number of items allowed in a checkpoint."
              << std::endl
              << "    chk_period                   - Time bound (in sec.) on a checkpoint."
              << std::endl
              << "    item_num_based_new_chk       - true if a new checkpoint can be created based"
              << std::endl
              << "                                   on." << std::endl
              << "                                   the number of items in the open checkpoint."
              << std::endl
              << "    keep_closed_chks             - true if we want to keep closed checkpoints in"
              << std::endl
              << "                                   memory." << std::endl
              << "                                   as long as the current memory usage is below"
              << std::endl
              << "                                   high water mark."
              << std::endl
              << "    max_checkpoints              - Max number of checkpoints allowed per vbucket."
              << std::endl
              << "    enable_chk_merge             = True if merging closed checkpoints is enabled."
              << std::endl
              << std::endl
              << std::endl
              << "  Available params for set flush_param:" << std::endl
              << "    access_scanner_enabled       - Enable or disable access scanner task (true/false)"
              << std::endl
              << "    alog_sleep_time              - Access scanner interval (minute)"
              << std::endl
              << "    alog_task_time               - Hour in UTC time when access scanner task is"
              << std::endl
              << "                                   next scheduled to run (0-23)."
              << std::endl
              << "    backfill_mem_threshold       - Memory threshold (%) on the current bucket quota"
              << std::endl
              << "                                   before backfill task is made to back off."
              << std::endl
              << "    bg_fetch_delay               - Delay before executing a bg fetch (test"
              << std::endl
              << "                                   feature)." << std::endl
              << "    bfilter_enabled              - Enable or disable bloom filters (true/false)"
              << std::endl
              << "    bfilter_residency_threshold  - Resident ratio threshold below which all items"
              << std::endl
              << "                                   will be considered in the bloom filters in full"
              << std::endl
              << "                                   eviction policy (0.0 - 1.0)"
              << std::endl
              << "    compaction_exp_mem_threshold - Memory threshold (%) on the current bucket quota"
              << std::endl
              << "                                   after which compaction will not queue expired"
              << std::endl
              << "                                   items for deletion."
              << std::endl
              << "    compaction_write_queue_cap   - Disk write queue threshold after which compaction"
              << std::endl
              << "                                   tasks will be made to snooze, if there are already"
              << std::endl
              << "                                   pending compaction tasks."
              << std::endl
              << "    dcp_min_compression_ratio    - Minimum compression ratio of compressed doc against"
              << std::endl
              << "                                   the original doc. If compressed doc is greater than"
              << std::endl
              << "                                   this percentage of the original doc, then the doc"
              << std::endl
              << "                                   will be shipped as is by the DCP producer if value"
              << std::endl
              << "                                   compression is enabled by the DCP consumer. Applies"
              << std::endl
              << "                                   to all producers (Ideal range: 0.0 - 1.0)"
              << std::endl
              << "    defragmenter_enabled         - Enable or disable the defragmenter"
              << std::endl
              << "                                   (true/false)." << std::endl
              << "    defragmenter_interval        - How often defragmenter task should be run"
              << std::endl
              << "                                   (in seconds)." << std::endl
              << "    defragmenter_age_threshold   - How old (measured in number of defragmenter"
              << std::endl
              << "                                   passes) must a document be to be considered"
              << std::endl
              << "                                   for defragmentation."
              << std::endl
              << "    defragmenter_chunk_duration  - Maximum time (in ms) defragmentation task"
              << std::endl
              << "                                   will run for before being paused (and"
              << std::endl
              << "                                   resumed at the next defragmenter_interval)."
              << std::endl
              << "    exp_pager_enabled            - Enable expiry pager."
              << std::endl
              << "    exp_pager_stime              - Expiry Pager Sleeptime."
              << std::endl
              << "    exp_pager_initial_run_time   - Expiry Pager first task time (UTC)"
              << std::endl
              << "                                   (Range: 0 - 23, Specify 'disable' to not delay the"
              << std::endl
              << "                                   the expiry pager, in which case first run will be"
              << std::endl
              << "                                   after exp_pager_stime seconds.)"
              << std::endl
              << "    flushall_enabled             - Enable flush operation."
              << std::endl
              << "    pager_active_vb_pcnt         - Percentage of active vbuckets items among"
              << std::endl
              << "                                   all ejected items by item pager."
              << std::endl
              << "    max_size                     - Max memory used by the server."
              << std::endl
              << "    mem_high_wat                 - High water mark (suffix with '%' to make it a"
              << std::endl
              << "                                   percentage of the RAM quota)"
              << std::endl
              << "    mem_low_wat                  - Low water mark. (suffix with '%' to make it a"
              << std::endl
              << "                                   percentage of the RAM quota)"
              << std::endl
              << "    mutation_mem_threshold       - Memory threshold (%) on the current bucket quota"
              << std::endl
              << "                                   for accepting a new mutation."
              << std::endl
              << "    timing_log                   - path to log detailed timing stats."
              << std::endl
              << "    warmup_min_memory_threshold  - Memory threshold (%) during warmup to enable"
              << std::endl
              << "                                   traffic" << std::endl
              << "    warmup_min_items_threshold   - Item number threshold (%) during warmup to enable"
              << std::endl
              << "                                   traffic" << std::endl
              << "    max_num_readers              - Override default number of global threads that"
              << std::endl
              << "                                   prioritize read operations."
              << std::endl
              << "    max_num_writers              - Override default number of global threads that"
              << std::endl
              << "                                   prioritize write operations."
              << std::endl
              << "    max_num_auxio                - Override default number of global threads that"
              << std::endl
              << "                                   prioritize auxio operations."
              << std::endl
              << "    max_num_nonio                - Override default number of global threads that"
              << std::endl
              << "                                   prioritize nonio operations."
              << std::endl
              << std::endl
              << "  Available params for set tap_param:" << std::endl
              << "    tap_keepalive                    - Seconds to hold a named tap connection."
              << std::endl
              << "    replication_throttle_queue_cap   - Max disk write queue size to throttle"
              << std::endl
              << "                                       replication streams ('infinite' means no"
              << std::endl
              << "                                       cap)." << std::endl
              << "    replication_throttle_cap_pcnt    - Percentage of total items in write queue"
              << std::endl
              << "                                       at which we throttle replication input"
              << std::endl
              << "    replication_throttle_threshold   - Percentage of memory in use to throttle"
              << std::endl
              << "                                       replication streams."
              << std::endl
              << std::endl
              << "  Available params for set dcp_param:" << std::endl
              << "    dcp_consumer_process_buffered_messages_yield_limit - The threshold at which"
              << std::endl
              << "                                                         the Consumer will yield"
              << std::endl
              << "                                                         when processing items."
              << std::endl
              << std::endl
              << "    dcp_consumer_process_buffered_messages_batch_size - The number of items the"
              << std::endl
              << "                                                        DCP processor will consume"
              << std::endl
              << "                                                        in a single batch."
              << std::endl
              << std::endl
              << "Available params for set_vbucket_param:" << std::endl
              << "    max_cas - Change the max_cas of a vbucket. The value and vbucket are specified as decimal"
              << std::endl
              << "              integers. The new-value is interpretted as an unsigned 64-bit integer."
              << std::endl
              << std::endl
              << "              mcepctl [options] set_vbucket_param max_cas <vbucket-id> <new-value>"
              << std::endl;
    exit(EXIT_FAILURE);
}

static std::string getStat(MemcachedBinprotConnection& connection,
                           const std::string& stat) {
    auto stats = connection.statsMap("");
    return stats[stat];
}

void handle_stop_persistence(MemcachedBinprotConnection& connection,
                             int, char**) {
    // Trying to stop it if it is already stopped returns EINVAL
    if (getStat(connection, "ep_flusher_state") == "paused") {
        std::cout << "Persistence already paused" << std::endl;
        return;
    }

    BinprotGenericCommand cmd(PROTOCOL_BINARY_CMD_STOP_PERSISTENCE);
    connection.sendCommand(cmd);

    BinprotResponse response;
    connection.recvResponse(response);
    if (!response.isSuccess()) {
        std::cout << "Error: "
                  << memcached_status_2_text(response.getStatus())
                  << std::endl;
        return;
    }

    // Wait for it to stop:
    while (getStat(connection, "ep_flusher_state") != "paused") {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    std::cout << "Persistence stopped" << std::endl;
}

void handle_start_persistence(MemcachedBinprotConnection& connection,
                              int, char**) {
    if (getStat(connection, "ep_flusher_state") == "running") {
        std::cout << "Persistence already running" << std::endl;
        return;
    }

    BinprotGenericCommand cmd(PROTOCOL_BINARY_CMD_START_PERSISTENCE);
    connection.sendCommand(cmd);

    BinprotResponse response;
    connection.recvResponse(response);
    if (response.isSuccess()) {
        std::cout << "Persistence started" << std::endl;
    } else {
        std::cout << "Error: "
                  << memcached_status_2_text(response.getStatus())
                  << std::endl;
    }
}

void handle_drain_persistence(MemcachedBinprotConnection& connection,
                              int, char**) {
    if (getStat(connection, "ep_flusher_state") != "running") {
        std::cerr << "Can't wait for the persistence to drain. Flusher state: "
                  << getStat(connection, "ep_flusher_state") << std::endl;
        return;
    }

    while (true) {
        if (getStat(connection, "ep_queue_size") == "0") {
            std::cout << "done" << std::endl;
            return;
        } else {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            std::cout << ".";
            std::cout.flush();
        }
    }
}

void handle_set(MemcachedBinprotConnection& connection,
                int argc, char** argv) {
    if ((optind + 4) != argc) {
        usage();
    }

    std::string type{argv[optind + 1]};
    std::string key{argv[optind + 2]};
    std::string value{argv[optind + 3]};

    protocol_binary_engine_param_t param;

    if (type == "checkpoint_param") {
        param = protocol_binary_engine_param_checkpoint;
    } else if (type == "flush_param") {
        param = protocol_binary_engine_param_flush;
    } else if (type == "tap_param") {
        param = protocol_binary_engine_param_tap;
    } else if (type == "dcp_param") {
        param = protocol_binary_engine_param_dcp;
    } else if (type == "vbucket_param") {
        param = protocol_binary_engine_param_vbucket;
    } else {
        usage();
        return;
    }

    if (key == "replication_throttle_queue_cap" && value == "infinite") {
        value = "-1";
    }

    if (key == "exp_pager_initial_run_time" && value == "disable") {
        value = "-1";
    }

    if (key == "mem_high_wat" || key == "mem_low_wat") {
        auto index = value.find("%");
        if (index != value.npos) {
            auto size = std::stoull(getStat(connection, "ep_max_size"));
            auto prc = std::stoul(value.substr(0, index));
            value = std::to_string(size_t((size * prc / 100.0)));
        }
    }

    BinprotSetParamCommand cmd(param, key, value);
    connection.sendCommand(cmd);

    BinprotResponse response;
    connection.recvResponse(response);

    if (response.isSuccess()) {
        std::cout << "set " << key << " to " << value << std::endl;
    } else {
        std::cout << "Error: "
                  << memcached_status_2_text(response.getStatus())
                  << std::endl;
    }
}

void handle_set_vbucket_param(MemcachedBinprotConnection& connection,
                              int argc, char** argv) {
    if ((optind + 4) != argc) {
        usage();
    }

    std::string subcommand{argv[optind + 1]};
    std::string vbucketid{argv[optind + 2]};
    std::string value{argv[optind + 3]};

    if (subcommand != "max_cas") {
        usage();
    }

    BinprotSetParamCommand cmd(protocol_binary_engine_param_vbucket,
                               subcommand, value);
    cmd.setVBucket(uint16_t(std::stoi(vbucketid)));
    connection.sendCommand(cmd);

    BinprotResponse response;
    connection.recvResponse(response);

    if (response.isSuccess()) {
        std::cout << "set " << subcommand << " to " << value << std::endl;
    } else {
        std::cout << "Error: "
                  << memcached_status_2_text(response.getStatus())
                  << std::endl;
    }
}

using CallbackFunc = std::function<void(MemcachedBinprotConnection& connection,
                                        int, char**)>;

const std::map<std::string, CallbackFunc> commandmap = {
    {"stop",              handle_stop_persistence},
    {"start",             handle_start_persistence},
    {"drain",             handle_drain_persistence},
    {"set",               handle_set},
    {"set_vbucket_param", handle_set_vbucket_param}
};

int main(int argc, char** argv) {
    int cmd;
    std::string port{"11210"};
    std::string host{"localhost"};
    std::string user{};
    std::string password{};
    std::string bucket{};
    sa_family_t family = AF_UNSPEC;
    bool secure = false;

    /* Initialize the socket subsystem */
    cb_initialize_sockets();

    while ((cmd = getopt(argc, argv, "46h:p:u:b:P:s")) != EOF) {
        switch (cmd) {
        case '6' :
            family = AF_INET6;
            break;
        case '4' :
            family = AF_INET;
            break;
        case 'h' :
            host.assign(optarg);
            break;
        case 'p':
            port.assign(optarg);
            break;
        case 'b' :
            bucket.assign(optarg);
            break;
        case 'u' :
            user.assign(optarg);
            break;
        case 'P':
            password.assign(optarg);
            break;
        case 's':
            secure = true;
            break;
        default:
            usage();
        }
    }

    // We need a command
    if ((optind + 1) > argc) {
        usage();
    }

    auto command = commandmap.find(argv[optind]);
    if (command == commandmap.end()) {
        usage();
    }

    try {
        in_port_t in_port;
        sa_family_t fam;
        std::tie(host, in_port, fam) = cb::inet::parse_hostname(host, port);

        if (family == AF_UNSPEC) { // The user may have used -4 or -6
            family = fam;
        }

        MemcachedBinprotConnection connection(host,
                                              in_port,
                                              family,
                                              secure);
        // MEMCACHED_VERSION contains the git sha
        connection.hello("mcepctl",
                         MEMCACHED_VERSION,
                         "command line utility to tune ep-engine");
        connection.setXerrorSupport(true);

        if (!user.empty()) {
            connection.authenticate(user, password,
                                    connection.getSaslMechanisms());
        }

        if (!bucket.empty()) {
            connection.selectBucket(bucket);
        }

        command->second(connection, argc, argv);
    } catch (const ConnectionError& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    } catch (const std::runtime_error& ex) {
        std::cerr << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
