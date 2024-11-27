/*
 *     Copyright 2016-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#pragma once

#include <memcached/engine_error.h>
#include <nlohmann/json_fwd.hpp>
#include <memory>
#include <unordered_set>

class StatCollector;
class CookieIface;
class AuditEventFilter;

namespace cb::audit {
namespace document {
enum class Operation { Read, Lock, Modify, Delete };
} // namespace document

class Audit {
public:
    virtual ~Audit() = default;

    /**
     * method called from the core to collect statistics information from
     * the audit subsystem
     *
     * @param add_stats a callback function to add information to the response
     * @param cookie the cookie representing the command
     */
    virtual void stats(const StatCollector& collector) = 0;

    /**
     * Put an audit event into the audit trail
     *
     * @param eventid The identifier for the event to insert
     * @param payload the JSON encoded payload to insert to the audit trail
     * @return true if the event was successfully added to the audit
     *              queue (may be dropped at a later time)
     *         false if an error occurred while trying to insert the
     *               event to the audit queue.
     */
    virtual bool put_event(uint32_t eventid, nlohmann::json payload) = 0;

    /**
     * Update the audit daemon with the specified configuration file
     *
     * @param config the configuration file to use.
     * @param cookie the command cookie to notify when we're done
     * @return true if success (and the cookie will be signalled when
     * reconfigure is complete)
     */
    virtual bool configure_auditdaemon(std::string config,
                                       CookieIface& cookie) = 0;

    /// Create an audit filter based upon the current configuration one may
    /// use to check if an event should be filtered out or not. Multiple
    /// threads may keep their own copy of the audit filter and perform
    /// filtering without having to obtain any locks in the case where
    /// an event should be filtered out.
    virtual std::unique_ptr<AuditEventFilter> createAuditEventFilter() = 0;

    /// Iterate over the audit trail on disk and generate a list of the DEKs
    /// in use in any of the files
    virtual std::unordered_set<std::string> get_deks_in_use() const = 0;

protected:
    Audit() = default;
};

using UniqueAuditPtr = std::unique_ptr<Audit>;

/**
 * create a new instance of the Audit subsystem
 *
 * @param config_file where to read the configuration
 * @return The newly created audit daemon handle upon success
 * @throws std::runtime_error if there is a failure allocating a daemon thread
 * @throws std::logic_error if there is something wrong with the config file
 * @throws std::bad_alloc for allocation failures
 */
UniqueAuditPtr create_audit_daemon(std::string config_file);

} // namespace cb::audit
