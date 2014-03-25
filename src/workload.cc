/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2014 Couchbase, Inc.
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

#include "config.h"

#include "workload.h"

#include "sigar.h"

const size_t default_shard_count(4);

WorkLoadPolicy::WorkLoadPolicy(int m, int s,
                               const std::string &pathSample) :
    maxNumWorkers(m)
{
    if (s) {
        maxNumShards = s;
        LOG(EXTENSION_LOG_WARNING,
                "Shard count set to %d by user configuration\n", s);
    } else {
        maxNumShards = getShardCount(pathSample.c_str());
        if (!maxNumShards) {
            maxNumShards = default_shard_count;
            LOG(EXTENSION_LOG_WARNING,
                    "Shard count defaulted to %d\n", maxNumShards);
        }
    }
}

/**
 * Estimate the number of shards based on the disk type (SSDs/HDDs)
 */
int WorkLoadPolicy::getShardCount(const char *path) {
    sigar_t *sigar;
    sigar_file_system_list_t fslist;

    if (path[0] &&
        sigar_open(&sigar) == SIGAR_OK &&
        sigar_file_system_list_get(sigar, &fslist) == SIGAR_OK) {

        sigar_file_system_usage_t fsu;
        int best_match = 0;
        int i,j;
        memset(&fsu, 0, sizeof(sigar_file_system_usage_t));
        /*
         * The following loop is to search for the correct filesystem,
         * among a list of filesystems, corresponding to the given path.
         * sigar_file_system_usage_get records the read-write times
         * for the selected filesystem.
         */
        for (i = 0, j = 0; i < fslist.number; i++) {
            sigar_file_system_t fs = fslist.data[i];
            if (strstr(path, fs.dir_name)) {
                size_t len = strlen(fs.dir_name);
                if (len > best_match) {
                    best_match = len;
                    sigar_file_system_usage_t fsusage;
                    if (sigar_file_system_usage_get(sigar, fs.dir_name,
                                                    &fsusage) == SIGAR_OK) {
                        fsu = fsusage;
                    }
                }
            }
        }

        sigar_file_system_list_destroy(sigar, &fslist);
        sigar_close(sigar);

        LOG(EXTENSION_LOG_WARNING,
                "Disk Times: Read=" SIGAR_F_U64 ", Write=" SIGAR_F_U64
                ", queue=" SIGAR_F_U64 ", time=" SIGAR_F_U64
                ", snaptime=" SIGAR_F_U64 ", service time=%g, queueDepth=%g\n",
                fsu.disk.rtime, fsu.disk.wtime, fsu.disk.queue, fsu.disk.time,
                fsu.disk.snaptime, fsu.disk.service_time, fsu.disk.queue);
        /*
         * As only SSDs have differential read-write times, and HDDs
         * do not, the following is to essentially differentiate
         * between SSDs and HDDs.
         */
        if (fsu.disk.wtime > (fsu.disk.rtime + (fsu.disk.rtime >> 1))) {
            LOG(EXTENSION_LOG_WARNING, "Shard count picked as 4\n");
            return 4;
        } else {
            LOG(EXTENSION_LOG_WARNING, "Shard count picked as 2\n");
            return 2;
        }
    }
    return 0;
}

