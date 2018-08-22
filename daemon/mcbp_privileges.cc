/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc.
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
#include "mcbp_privileges.h"
#include <memcached/protocol_binary.h>
#include "connection.h"

using namespace cb::rbac;

PrivilegeAccess McbpPrivilegeChains::invoke(protocol_binary_command command,
                                            Cookie& cookie) {
    auto& chain = commandChains[command];
    if (chain.empty()) {
        return cb::rbac::PrivilegeAccess::Fail;
    } else {
        try {
            return chain.invoke(cookie);
        } catch (const std::bad_function_call&) {
            LOG_WARNING(
                    "{}: bad_function_call caught while evaluating access "
                    "control for opcode: {:x}",
                    cookie.getConnection().getId(),
                    command);
            // Let the connection catch the exception and shut down the
            // connection
            throw;
        }
    }
}

template <Privilege T>
static PrivilegeAccess require(Cookie& cookie) {
    return cookie.getConnection().checkPrivilege(T, cookie);
}

static PrivilegeAccess requireInsertOrUpsert(Cookie& cookie) {
    auto ret = cookie.getConnection().checkPrivilege(Privilege::Insert, cookie);
    if (ret == PrivilegeAccess::Ok) {
        return PrivilegeAccess::Ok;
    } else {
        return cookie.getConnection().checkPrivilege(Privilege::Upsert, cookie);
    }
}

static PrivilegeAccess empty(Cookie& cookie) {
    return PrivilegeAccess::Ok;
}

McbpPrivilegeChains::McbpPrivilegeChains() {

    setup(PROTOCOL_BINARY_CMD_GET, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_GETQ, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_GETK, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_GETKQ, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_GET_FAILOVER_LOG, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_SET, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_SETQ, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_ADD, requireInsertOrUpsert);
    setup(PROTOCOL_BINARY_CMD_ADDQ, requireInsertOrUpsert);
    setup(PROTOCOL_BINARY_CMD_REPLACE, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_REPLACEQ, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_DELETE, require<Privilege::Delete>);
    setup(PROTOCOL_BINARY_CMD_DELETEQ, require<Privilege::Delete>);
    setup(PROTOCOL_BINARY_CMD_APPEND, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_APPENDQ, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_PREPEND, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_PREPENDQ, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_INCREMENT, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_INCREMENT, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_INCREMENTQ, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_INCREMENTQ, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_DECREMENT, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_DECREMENT, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_DECREMENTQ, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_DECREMENTQ, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_QUIT, empty);
    setup(PROTOCOL_BINARY_CMD_QUITQ, empty);
    setup(PROTOCOL_BINARY_CMD_FLUSH, require<Privilege::BucketManagement>);
    setup(PROTOCOL_BINARY_CMD_FLUSHQ, require<Privilege::BucketManagement>);
    setup(PROTOCOL_BINARY_CMD_NOOP, empty);
    setup(PROTOCOL_BINARY_CMD_VERSION, empty);
    setup(PROTOCOL_BINARY_CMD_STAT, require<Privilege::SimpleStats>);
    setup(PROTOCOL_BINARY_CMD_VERBOSITY, require<Privilege::NodeManagement>);
    setup(PROTOCOL_BINARY_CMD_TOUCH, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_GAT, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_GAT, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_GATQ, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_GATQ, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_HELLO, empty);
    setup(PROTOCOL_BINARY_CMD_GET_ERROR_MAP, empty);
    setup(PROTOCOL_BINARY_CMD_SASL_LIST_MECHS, empty);
    setup(PROTOCOL_BINARY_CMD_SASL_AUTH, empty);
    setup(PROTOCOL_BINARY_CMD_SASL_STEP, empty);
    /* Control */
    setup(PROTOCOL_BINARY_CMD_IOCTL_GET, require<Privilege::NodeManagement>);
    setup(PROTOCOL_BINARY_CMD_IOCTL_SET, require<Privilege::NodeManagement>);

    /* Config */
    setup(PROTOCOL_BINARY_CMD_CONFIG_VALIDATE, require<Privilege::NodeManagement>);
    setup(PROTOCOL_BINARY_CMD_CONFIG_RELOAD, require<Privilege::NodeManagement>);

    /* Audit */
    setup(PROTOCOL_BINARY_CMD_AUDIT_PUT, require<Privilege::Audit>);
    setup(PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD,
          require<Privilege::AuditManagement>);

    /* Shutdown the server */
    setup(PROTOCOL_BINARY_CMD_SHUTDOWN, require<Privilege::NodeManagement>);

    /* VBucket commands */
    setup(PROTOCOL_BINARY_CMD_SET_VBUCKET, require<Privilege::BucketManagement>);
    // The testrunner client seem to use this command..
    setup(PROTOCOL_BINARY_CMD_GET_VBUCKET, empty);
    setup(PROTOCOL_BINARY_CMD_DEL_VBUCKET, require<Privilege::BucketManagement>);
    /* End VBucket commands */

    /* TAP commands */
    /* We want to return PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED */
    setup(PROTOCOL_BINARY_CMD_TAP_CONNECT, empty);
    setup(PROTOCOL_BINARY_CMD_TAP_MUTATION, empty);
    setup(PROTOCOL_BINARY_CMD_TAP_DELETE, empty);
    setup(PROTOCOL_BINARY_CMD_TAP_FLUSH, empty);
    setup(PROTOCOL_BINARY_CMD_TAP_OPAQUE, empty);
    setup(PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET, empty);
    setup(PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START, empty);
    setup(PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END, empty);
    /* End TAP */

    /* Vbucket command to get the VBUCKET sequence numbers for all
     * vbuckets on the node */
    setup(PROTOCOL_BINARY_CMD_GET_ALL_VB_SEQNOS, require<Privilege::MetaRead>);

    /* DCP */
    setup(PROTOCOL_BINARY_CMD_DCP_OPEN, empty);
    setup(PROTOCOL_BINARY_CMD_DCP_ADD_STREAM, require<Privilege::DcpProducer>);
    setup(PROTOCOL_BINARY_CMD_DCP_CLOSE_STREAM, require<Privilege::DcpProducer>);
    setup(PROTOCOL_BINARY_CMD_DCP_STREAM_REQ, require<Privilege::DcpProducer>);
    setup(PROTOCOL_BINARY_CMD_DCP_GET_FAILOVER_LOG, require<Privilege::DcpProducer>);
    setup(PROTOCOL_BINARY_CMD_DCP_STREAM_END, require<Privilege::DcpConsumer>);
    setup(PROTOCOL_BINARY_CMD_DCP_SNAPSHOT_MARKER, require<Privilege::DcpConsumer>);
    setup(PROTOCOL_BINARY_CMD_DCP_MUTATION, require<Privilege::DcpConsumer>);
    setup(PROTOCOL_BINARY_CMD_DCP_DELETION, require<Privilege::DcpConsumer>);
    setup(PROTOCOL_BINARY_CMD_DCP_EXPIRATION, require<Privilege::DcpConsumer>);
    setup(PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE, require<Privilege::DcpConsumer>);
    setup(PROTOCOL_BINARY_CMD_DCP_NOOP, empty);
    setup(PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT, empty);
    setup(PROTOCOL_BINARY_CMD_DCP_CONTROL, empty);
    setup(PROTOCOL_BINARY_CMD_DCP_SYSTEM_EVENT,
          require<Privilege::DcpConsumer>);
    /* End DCP */

    setup(PROTOCOL_BINARY_CMD_STOP_PERSISTENCE,
          require<Privilege::NodeManagement>);
    setup(PROTOCOL_BINARY_CMD_START_PERSISTENCE,
          require<Privilege::NodeManagement>);
    setup(PROTOCOL_BINARY_CMD_SET_PARAM, require<Privilege::NodeManagement>);
    setup(PROTOCOL_BINARY_CMD_GET_REPLICA, require<Privilege::Read>);

    /* Bucket engine */
    setup(PROTOCOL_BINARY_CMD_CREATE_BUCKET, require<Privilege::BucketManagement>);
    setup(PROTOCOL_BINARY_CMD_DELETE_BUCKET, require<Privilege::BucketManagement>);
    // Everyone should be able to list their own buckets
    setup(PROTOCOL_BINARY_CMD_LIST_BUCKETS, empty);
    // And select the one they have access to
    setup(PROTOCOL_BINARY_CMD_SELECT_BUCKET, empty);

    setup(PROTOCOL_BINARY_CMD_OBSERVE_SEQNO, require<Privilege::MetaRead>);
    setup(PROTOCOL_BINARY_CMD_OBSERVE, require<Privilege::MetaRead>);

    setup(PROTOCOL_BINARY_CMD_EVICT_KEY, require<Privilege::NodeManagement>);
    setup(PROTOCOL_BINARY_CMD_GET_LOCKED, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_UNLOCK_KEY, require<Privilege::Read>);

    /**
     * Return the last closed checkpoint Id for a given VBucket.
     */
    setup(PROTOCOL_BINARY_CMD_LAST_CLOSED_CHECKPOINT, require<Privilege::MetaRead>);

    /**
     * CMD_GET_META is used to retrieve the meta section for an item.
     */
    setup(PROTOCOL_BINARY_CMD_GET_META, require<Privilege::MetaRead>);
    setup(PROTOCOL_BINARY_CMD_GETQ_META, require<Privilege::MetaRead>);
    setup(PROTOCOL_BINARY_CMD_SET_WITH_META, require<Privilege::MetaWrite>);
    setup(PROTOCOL_BINARY_CMD_SETQ_WITH_META, require<Privilege::MetaWrite>);
    setup(PROTOCOL_BINARY_CMD_ADD_WITH_META, require<Privilege::MetaWrite>);
    setup(PROTOCOL_BINARY_CMD_ADDQ_WITH_META, require<Privilege::MetaWrite>);
    setup(PROTOCOL_BINARY_CMD_SNAPSHOT_VB_STATES, require<Privilege::MetaWrite>);
    setup(PROTOCOL_BINARY_CMD_VBUCKET_BATCH_COUNT, require<Privilege::MetaWrite>);
    setup(PROTOCOL_BINARY_CMD_DEL_WITH_META, require<Privilege::MetaWrite>);
    setup(PROTOCOL_BINARY_CMD_DELQ_WITH_META, require<Privilege::MetaWrite>);

    /**
     * Command to create a new checkpoint on a given vbucket by force
     */
    setup(PROTOCOL_BINARY_CMD_CREATE_CHECKPOINT, require<Privilege::NodeManagement>);
    setup(PROTOCOL_BINARY_CMD_NOTIFY_VBUCKET_UPDATE, require<Privilege::MetaWrite>);
    /**
     * Command to enable data traffic after completion of warm
     */
    setup(PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC,
          require<Privilege::NodeManagement>);
    /**
     * Command to disable data traffic temporarily
     */
    setup(PROTOCOL_BINARY_CMD_DISABLE_TRAFFIC,
          require<Privilege::NodeManagement>);
    /**
     * Command to wait for the checkpoint persistence
     */
    setup(PROTOCOL_BINARY_CMD_CHECKPOINT_PERSISTENCE, require<Privilege::NodeManagement>);
    /**
     * Command that returns meta data for typical memcached ops
     */
    setup(PROTOCOL_BINARY_CMD_RETURN_META, require<Privilege::MetaRead>);
    setup(PROTOCOL_BINARY_CMD_RETURN_META, require<Privilege::MetaWrite>);
    /**
     * Command to trigger compaction of a vbucket
     */
    setup(PROTOCOL_BINARY_CMD_COMPACT_DB, require<Privilege::NodeManagement>);
    /**
     * Command to set cluster configuration
     */
    setup(PROTOCOL_BINARY_CMD_SET_CLUSTER_CONFIG, require<Privilege::SecurityManagement>);
    /**
     * Command that returns cluster configuration (open to anyone)
     */
    setup(PROTOCOL_BINARY_CMD_GET_CLUSTER_CONFIG, empty);

    setup(PROTOCOL_BINARY_CMD_GET_RANDOM_KEY, require<Privilege::Read>);
    /**
     * Command to wait for the dcp sequence number persistence
     */
    setup(PROTOCOL_BINARY_CMD_SEQNO_PERSISTENCE, require<Privilege::NodeManagement>);
    /**
     * Command to get all keys
     */
    setup(PROTOCOL_BINARY_CMD_GET_KEYS, require<Privilege::Read>);
    /**
     * Commands for GO-XDCR
     */
    setup(PROTOCOL_BINARY_CMD_SET_DRIFT_COUNTER_STATE, require<Privilege::NodeManagement>);
    setup(PROTOCOL_BINARY_CMD_GET_ADJUSTED_TIME, require<Privilege::NodeManagement>);

    /**
     * Commands for the Sub-document API.
     */

    /* Retrieval commands */
    setup(PROTOCOL_BINARY_CMD_SUBDOC_GET, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_EXISTS, require<Privilege::Read>);

    /* Dictionary commands */
    setup(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT, require<Privilege::Upsert>);

    /* Generic modification commands */
    setup(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, require<Privilege::Upsert>);

    /* Array commands */
    setup(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE, require<Privilege::Upsert>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT, require<Privilege::Read>);

    /* Arithmetic commands */
    setup(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER, require<Privilege::Upsert>);

    /* Multi-Path commands */
    setup(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION, require<Privilege::Upsert>);


    /* Scrub the data */
    setup(PROTOCOL_BINARY_CMD_SCRUB, require<Privilege::NodeManagement>);
    /* Refresh the ISASL data */
    setup(PROTOCOL_BINARY_CMD_ISASL_REFRESH,
          require<Privilege::SecurityManagement>);
    /* Refresh the SSL certificates */
    setup(PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH,
          require<Privilege::SecurityManagement>);
    /* Internal timer ioctl */
    setup(PROTOCOL_BINARY_CMD_GET_CMD_TIMER, empty);
    /* ns_server - memcached session validation */
    setup(PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN, require<Privilege::SessionManagement>);
    setup(PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN, require<Privilege::SessionManagement>);

    // Drop a privilege from the effective set
    setup(PROTOCOL_BINARY_CMD_DROP_PRIVILEGE, empty);

    setup(uint8_t(cb::mcbp::ClientOpcode::UpdateUserPermissions),
          require<Privilege::SecurityManagement>);

    /* Refresh the RBAC data */
    setup(PROTOCOL_BINARY_CMD_RBAC_REFRESH,
          require<Privilege::SecurityManagement>);

    setup(uint8_t(cb::mcbp::ClientOpcode::AuthProvider),
          require<Privilege::SecurityManagement>);

    setup(uint8_t(cb::mcbp::ClientOpcode::GetActiveExternalUsers),
          require<Privilege::SecurityManagement>);

    /// @todo change priv to CollectionManagement
    setup(PROTOCOL_BINARY_CMD_COLLECTIONS_SET_MANIFEST,
          require<Privilege::BucketManagement>);

    /// all clients may need to read the manifest
    setup(PROTOCOL_BINARY_CMD_COLLECTIONS_GET_MANIFEST, empty);

    if (getenv("MEMCACHED_UNIT_TESTS") != nullptr) {
        // The opcode used to set the clock by our extension
        setup(protocol_binary_command(PROTOCOL_BINARY_CMD_ADJUST_TIMEOFDAY), empty);
        // The opcode used by ewouldblock
        setup(protocol_binary_command(PROTOCOL_BINARY_CMD_EWOULDBLOCK_CTL), empty);
        // We have a unit tests that tries to fetch this opcode to detect
        // that we don't crash (we used to have an array which was too
        // small ;-)
        setup(protocol_binary_command(PROTOCOL_BINARY_CMD_INVALID), empty);
    }

}
