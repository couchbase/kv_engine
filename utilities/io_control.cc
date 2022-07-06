/*
 *     Copyright 2022-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <memcached/io_control.h>
#include <stdexcept>

namespace cb::ioctl {
Manager::Manager()
    : entries({{Id::JemallocProfActive,
                "jemalloc.prof.active",
                "Request that jemalloc activates or deactivates profiling",
                Mode::WRONLY},
               {Id::JemallocProfDump,
                "jemalloc.prof.dump",
                "Request that jemalloc dumps the current profiling data",
                Mode::WRONLY},
               {Id::ReleaseFreeMemory,
                "release_free_memory",
                "Request the process to return unused memory to the operating "
                "system",
                Mode::WRONLY},
               {Id::ServerlessMaxConnectionsPerBucket,
                "serverless.max_connections_per_bucket",
                "Set the maximum number of external connections for a bucket",
                Mode::WRONLY},
               {Id::ServerlessReadUnitSize,
                "serverless.read_unit_size",
                "Set the size for the Read Unit (in bytes)",
                Mode::WRONLY},
               {Id::ServerlessWriteUnitSize,
                "serverless.write_unit_size",
                "Set the size for the Write Unit (in bytes)",
                Mode::WRONLY},
               {Id::Sla,
                "sla",
                "Get/Set the current SLA configuration",
                Mode::RW},
               {Id::TraceConfig,
                "trace.config",
                "Get/Set trace configuration",
                Mode::RW},
               {Id::TraceDumpBegin,
                "trace.dump.begin",
                "Generate a trace dump",
                Mode::RDONLY},
               {Id::TraceDumpClear,
                "trace.dump.clear",
                "Remove the trace represented with the provided UUID",
                Mode::WRONLY},
               {Id::TraceDumpGet,
                "trace.dump.get",
                "Retrieve the specified trace dump. Specified by appending "
                "'?id=[uuid]' to the key. Ex: "
                "'trace.dump.get?id=16dae44f-4e1f-4d1e-e31e-143bbc600907'",
                Mode::RDONLY},
               {Id::TraceDumpList,
                "trace.dump.list",
                "List all trace dumps stored on the server",
                Mode::RDONLY},
               {Id::TraceStart,
                "trace.start",
                "Start collecting trace information on the server",
                Mode::WRONLY},
               {Id::TraceStatus,
                "trace.status",
                "Request the status of the tracer",
                Mode::RDONLY},
               {Id::TraceStop,
                "trace.stop",
                "Stop collecting trace information on the server",
                Mode::WRONLY}}) {
}

const Manager& Manager::getInstance() {
    static Manager instance;
    return instance;
}

const Control* Manager::lookup(std::string_view key) const {
    for (const auto& e : entries) {
        if (e.key == key) {
            return &e;
        }
    }
    return nullptr;
}

const Control& Manager::lookup(Id id) const {
    for (const auto& e : entries) {
        if (e.id == id) {
            return e;
        }
    }
    throw std::invalid_argument(
            "Manager::lookup(Id id) called with invalid id: " +
            std::to_string(int(id)));
}

void Manager::iterate(std::function<void(const Control&)> callback) const {
    for (const auto& e : getInstance().entries) {
        callback(e);
    }
}

} // namespace cb::ioctl
