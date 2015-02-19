/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#ifndef AUDITCONFIG_H
#define AUDITCONFIG_H

#include <inttypes.h>
#include <string>
#include <vector>

class AuditConfig {
public:
    uint32_t rotate_interval;
    bool auditd_enabled;
    std::string log_path;
    std::string descriptors_path;
    std::vector<uint32_t> disabled;
    std::vector<uint32_t> sync;
    static uint32_t min_file_rotation_time;
    static uint32_t max_file_rotation_time;

    AuditConfig(void) : rotate_size(20 * 1024 * 1024), buffered(true) {
        auditd_enabled = false;
    }

    ~AuditConfig(void) {
        clean_up();
    }

    bool initialize_config(const std::string& str);
    void clean_up(void) {
        disabled.clear();
        sync.clear();
    }

    size_t get_rotate_size(void) const {
        return rotate_size;
    }

    bool is_buffered(void) const {
        return buffered;
    }

private:
    size_t rotate_size;
    bool buffered;
};

#endif
