/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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
#include "memcached.h"

using namespace cb::rbac;

template<Privilege T>
static PrivilegeAccess require(const Cookie& cookie) {
    if (cookie.connection == nullptr) {
        throw std::logic_error("haveRead: cookie.connection can't be null");
    }
    return cookie.connection->checkPrivilege(T);
}

static PrivilegeAccess empty(const Cookie& cookie) {
    return PrivilegeAccess::Ok;
}

McbpPrivilegeChains::McbpPrivilegeChains() {

    setup(PROTOCOL_BINARY_CMD_GET, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_GETQ, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_GETK, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_GETKQ, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_SET, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_SETQ, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_ADD, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_ADDQ, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_REPLACE, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_REPLACEQ, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_DELETE, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_DELETEQ, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_APPEND, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_APPENDQ, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_PREPEND, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_PREPENDQ, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_INCREMENT, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_INCREMENT, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_INCREMENTQ, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_INCREMENTQ, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_DECREMENT, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_DECREMENT, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_DECREMENTQ, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_DECREMENTQ, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_QUIT, empty);
    setup(PROTOCOL_BINARY_CMD_QUITQ, empty);
    setup(PROTOCOL_BINARY_CMD_FLUSH, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_FLUSHQ, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_NOOP, empty);
    setup(PROTOCOL_BINARY_CMD_VERSION, empty);
    setup(PROTOCOL_BINARY_CMD_STAT, require<Privilege::SimpleStats>);
    setup(PROTOCOL_BINARY_CMD_VERBOSITY, require<Privilege::NodeManagement>);
    setup(PROTOCOL_BINARY_CMD_TOUCH, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_GAT, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_GAT, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_GATQ, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_GATQ, require<Privilege::Write>);
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
    setup(PROTOCOL_BINARY_CMD_AUDIT_CONFIG_RELOAD, require<Privilege::NodeManagement>);

    /* Shutdown the server */
    setup(PROTOCOL_BINARY_CMD_SHUTDOWN, require<Privilege::NodeManagement>);

    /* VBucket commands */
    setup(PROTOCOL_BINARY_CMD_SET_VBUCKET, require<Privilege::BucketManagement>);
    // The testrunner client seem to use this command..
    setup(PROTOCOL_BINARY_CMD_GET_VBUCKET, empty);
    setup(PROTOCOL_BINARY_CMD_DEL_VBUCKET, require<Privilege::BucketManagement>);
    /* End VBucket commands */

    /* TAP commands */
    setup(PROTOCOL_BINARY_CMD_TAP_CONNECT, require<Privilege::TapProducer>);
    setup(PROTOCOL_BINARY_CMD_TAP_MUTATION, require<Privilege::TapConsumer>);
    setup(PROTOCOL_BINARY_CMD_TAP_DELETE, require<Privilege::TapConsumer>);
    setup(PROTOCOL_BINARY_CMD_TAP_FLUSH, require<Privilege::TapConsumer>);
    // TAP_OPAQUE is used by both the consumer and the producer so
    // ep-engine needs to perform the privilege check
    setup(PROTOCOL_BINARY_CMD_TAP_OPAQUE, empty);
    setup(PROTOCOL_BINARY_CMD_TAP_VBUCKET_SET, require<Privilege::TapConsumer>);
    setup(PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_START, require<Privilege::TapConsumer>);
    setup(PROTOCOL_BINARY_CMD_TAP_CHECKPOINT_END, require<Privilege::TapConsumer>);
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
    setup(PROTOCOL_BINARY_CMD_DCP_FLUSH, require<Privilege::DcpConsumer>);
    setup(PROTOCOL_BINARY_CMD_DCP_SET_VBUCKET_STATE, require<Privilege::DcpConsumer>);
    setup(PROTOCOL_BINARY_CMD_DCP_NOOP, empty);
    setup(PROTOCOL_BINARY_CMD_DCP_BUFFER_ACKNOWLEDGEMENT, empty);
    setup(PROTOCOL_BINARY_CMD_DCP_CONTROL, empty);
    /* End DCP */

    setup(PROTOCOL_BINARY_CMD_STOP_PERSISTENCE, require<Privilege::NodeManagement>);
    setup(PROTOCOL_BINARY_CMD_START_PERSISTENCE, require<Privilege::NodeManagement>);
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
     * Close the TAP connection for the registered TAP client and
     * remove the checkpoint cursors from its registered vbuckets.
     */
    setup(PROTOCOL_BINARY_CMD_DEREGISTER_TAP_CLIENT, require<Privilege::TapProducer>);

    /**
     * Reset the replication chain from the node that receives
     * this command. For example, given the replication chain,
     * A->B->C, if A receives this command, it will reset all the
     * replica vbuckets on B and C, which are replicated from A.
     */
    setup(PROTOCOL_BINARY_CMD_RESET_REPLICATION_CHAIN, require<Privilege::NodeManagement>);

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
    setup(PROTOCOL_BINARY_CMD_ENABLE_TRAFFIC, require<Privilege::MetaWrite>);
    /**
     * Command to disable data traffic temporarily
     */
    setup(PROTOCOL_BINARY_CMD_DISABLE_TRAFFIC, require<Privilege::MetaWrite>);
    /**
     * Command to change the vbucket filter for a given TAP producer.
     */
    setup(PROTOCOL_BINARY_CMD_CHANGE_VB_FILTER, require<Privilege::TapProducer>);
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
    setup(PROTOCOL_BINARY_CMD_SET_CLUSTER_CONFIG, require<Privilege::NodeManagement>);
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
    setup(PROTOCOL_BINARY_CMD_SUBDOC_DICT_ADD, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_DICT_UPSERT, require<Privilege::Write>);

    /* Generic modification commands */
    setup(PROTOCOL_BINARY_CMD_SUBDOC_DELETE, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_REPLACE, require<Privilege::Write>);

    /* Array commands */
    setup(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_LAST, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_PUSH_FIRST, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_INSERT, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_ARRAY_ADD_UNIQUE, require<Privilege::Write>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_GET_COUNT, require<Privilege::Read>);

    /* Arithmetic commands */
    setup(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_COUNTER, require<Privilege::Write>);

    /* Multi-Path commands */
    setup(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_LOOKUP, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION, require<Privilege::Read>);
    setup(PROTOCOL_BINARY_CMD_SUBDOC_MULTI_MUTATION, require<Privilege::Write>);


    /* Scrub the data */
    setup(PROTOCOL_BINARY_CMD_SCRUB, require<Privilege::NodeManagement>);
    /* Refresh the ISASL data */
    setup(PROTOCOL_BINARY_CMD_ISASL_REFRESH, require<Privilege::NodeManagement>);
    /* Refresh the SSL certificates */
    setup(PROTOCOL_BINARY_CMD_SSL_CERTS_REFRESH, require<Privilege::NodeManagement>);
    /* Internal timer ioctl */
    setup(PROTOCOL_BINARY_CMD_GET_CMD_TIMER, require<Privilege::NodeManagement>);
    /* ns_server - memcached session validation */
    setup(PROTOCOL_BINARY_CMD_SET_CTRL_TOKEN, require<Privilege::SessionManagement>);
    setup(PROTOCOL_BINARY_CMD_GET_CTRL_TOKEN, require<Privilege::SessionManagement>);

    /* ns_server - memcached internal communication */
    setup(PROTOCOL_BINARY_CMD_INIT_COMPLETE, require<Privilege::NodeManagement>);

    /* Refresh the RBAC data */
    setup(PROTOCOL_BINARY_CMD_RBAC_REFRESH, require<Privilege::NodeManagement>);


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
