/*
 *     Copyright 2015-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

/*
 *                "ewouldblock_engine"
 *
 * The "ewouldblock_engine" allows one to test how memcached responds when the
 * engine returns EWOULDBLOCK instead of the correct response.
 *
 * Motivation:
 *
 * The EWOULDBLOCK response code can be returned from a number of engine
 * functions, and is used to indicate that the request could not be immediately
 * fulfilled, and it "would block" if it tried to. The correct way for
 * memcached to handle this (in general) is to suspend that request until it
 * is later notified by the engine (via notify_io_complete()).
 *
 * However, engines typically return the correct response to requests
 * immediately, only rarely (and from memcached's POV non-deterministically)
 * returning EWOULDBLOCK. This makes testing of the code-paths handling
 * EWOULDBLOCK tricky.
 *
 *
 * Operation:
 * This engine, when loaded by memcached proxies requests to a "real" engine.
 * Depending on how it is configured, it can simply pass the request on to the
 * real engine, or artificially return EWOULDBLOCK back to memcached.
 *
 * See the 'Modes' enum below for the possible modes for a connection. The mode
 * can be selected by sending a `request_ewouldblock_ctl` command
 *  (opcode cb::mcbp::ClientOpcode::EwouldblockCtl).
 */

#include "ewouldblock_engine.h"
#include "ewouldblock_engine_public.h"
#include <daemon/enginemap.h>
#include <fmt/format.h>
#include <folly/CancellationToken.h>
#include <gsl/gsl-lite.hpp>
#include <logger/logger.h>
#include <memcached/collections.h>
#include <memcached/config_parser.h>
#include <memcached/connection_iface.h>
#include <memcached/cookie_iface.h>
#include <memcached/dcp.h>
#include <memcached/durability_spec.h>
#include <memcached/engine.h>
#include <memcached/range_scan_optional_configuration.h>
#include <memcached/server_bucket_iface.h>
#include <platform/dirutils.h>
#include <platform/thread.h>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <queue>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

static unique_engine_ptr createBucket(const std::string& module,
                                      ServerApi* (*get_server_api)()) {
    auto type = module_to_bucket_type(module);
    if (type == BucketType::Unknown) {
        return {};
    }

    try {
        return new_engine_instance(type, get_server_api);
    } catch (const std::exception&) {
        return {};
    }
}

/** ewouldblock_engine class */
class EWB_Engine : public EngineIface,
                   public DcpIface,
                   public DcpConnHandlerIface {
public:
    explicit EWB_Engine(GET_SERVER_API gsa_);

    ~EWB_Engine() override;

    /* Implementation of all the engine functions. ***************************/
    void initiate_shutdown() override;
    void disconnect(CookieIface& cookie) override;
    cb::engine_errc initialize(std::string_view config_str,
                               const nlohmann::json& encryption,
                               std::string_view chronicleAuthToken) override;
    void destroy(bool force) override;
    cb::engine_errc set_traffic_control_mode(CookieIface& cookie,
                                             TrafficControlMode mode) override;
    cb::unique_item_ptr allocateItem(CookieIface& cookie,
                                     const DocKeyView& key,
                                     size_t nbytes,
                                     size_t priv_nbytes,
                                     uint32_t flags,
                                     rel_time_t exptime,
                                     uint8_t datatype,
                                     Vbid vbucket) override;
    cb::engine_errc remove(
            CookieIface& cookie,
            const DocKeyView& key,
            uint64_t& cas,
            Vbid vbucket,
            const std::optional<cb::durability::Requirements>& durability,
            mutation_descr_t& mut_info) override;
    void release(ItemIface& item) override;
    cb::EngineErrorItemPair get(CookieIface& cookie,
                                const DocKeyView& key,
                                Vbid vbucket,
                                DocStateFilter documentStateFilter) override;
    cb::EngineErrorItemPair get_random_document(CookieIface& cookie,
                                                CollectionID cid) override;
    cb::EngineErrorItemPair get_if(
            CookieIface& cookie,
            const DocKeyView& key,
            Vbid vbucket,
            const std::function<bool(const item_info&)>& filter) override;
    cb::EngineErrorItemPair get_and_touch(
            CookieIface& cookie,
            const DocKeyView& key,
            Vbid vbucket,
            uint32_t exptime,
            const std::optional<cb::durability::Requirements>& durability)
            override;
    cb::EngineErrorItemPair get_locked(
            CookieIface& cookie,
            const DocKeyView& key,
            Vbid vbucket,
            std::chrono::seconds lock_timeout) override;
    cb::engine_errc unlock(CookieIface& cookie,
                           const DocKeyView& key,
                           Vbid vbucket,
                           uint64_t cas) override;
    cb::EngineErrorMetadataPair get_meta(CookieIface& cookie,
                                         const DocKeyView& key,
                                         Vbid vbucket) override;
    cb::engine_errc evict_key(CookieIface& cookie,
                              const DocKeyView& key,
                              Vbid vbucket) override;
    cb::engine_errc observe(
            CookieIface& cookie,
            const DocKeyView& key,
            Vbid vbucket,
            const std::function<void(uint8_t, uint64_t)>& key_handler,
            uint64_t& persist_time_hint) override;
    cb::engine_errc store(
            CookieIface& cookie,
            ItemIface& item,
            uint64_t& cas,
            StoreSemantics operation,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) override;
    cb::EngineErrorCasPair store_if(
            CookieIface& cookie,
            ItemIface& item,
            uint64_t cas,
            StoreSemantics operation,
            const cb::StoreIfPredicate& predicate,
            const std::optional<cb::durability::Requirements>& durability,
            DocumentState document_state,
            bool preserveTtl) override;
    cb::engine_errc flush(CookieIface& cookie) override;
    cb::engine_errc get_stats(CookieIface& cookie,
                              std::string_view key,
                              std::string_view value,
                              const AddStatFn& add_stat,
                              const CheckYieldFn& check_yield) override;
    void reset_stats(CookieIface& cookie) override;
    cb::engine_errc unknown_command(CookieIface& cookie,
                                    const cb::mcbp::Request& req,
                                    const AddResponseFn& response) override;
    bool get_item_info(const ItemIface& item, item_info& item_info) override;
    cb::engine_errc set_collection_manifest(CookieIface& cookie,
                                            std::string_view json) override;
    cb::engine_errc get_collection_manifest(
            CookieIface& cookie, const AddResponseFn& response) override;
    cb::EngineErrorGetCollectionIDResult get_collection_id(
            CookieIface& cookie, std::string_view path) override;
    cb::EngineErrorGetScopeIDResult get_scope_id(
            CookieIface& cookie, std::string_view path) override;
    cb::EngineErrorGetCollectionMetaResult get_collection_meta(
            CookieIface& cookie,
            CollectionID cid,
            std::optional<Vbid> vbid) const override;
    cb::engine::FeatureSet getFeatures() override;
    bool isXattrEnabled() override;
    std::optional<cb::HlcTime> getVBucketHlcNow(Vbid vbucket) override;
    BucketCompressionMode getCompressionMode() override;
    size_t getMaxItemSize() override;
    float getMinCompressionRatio() override;
    cb::engine_errc setParameter(CookieIface& cookie,
                                 EngineParamCategory category,
                                 std::string_view key,
                                 std::string_view value,
                                 Vbid vbucket) override;
    cb::engine_errc compactDatabase(
            CookieIface& cookie,
            Vbid vbid,
            uint64_t purge_before_ts,
            uint64_t purge_before_seq,
            bool drop_deletes,
            const std::vector<std::string>& obsolete_keys) override;
    std::pair<cb::engine_errc, vbucket_state_t> getVBucket(CookieIface& cookie,
                                                           Vbid vbid) override;
    cb::engine_errc setVBucket(CookieIface& cookie,
                               Vbid vbid,
                               uint64_t cas,
                               vbucket_state_t state,
                               nlohmann::json* meta) override;
    cb::engine_errc deleteVBucket(CookieIface& cookie,
                                  Vbid vbid,
                                  bool sync) override;
    std::pair<cb::engine_errc, cb::rangescan::Id> createRangeScan(
            CookieIface& cookie,
            const cb::rangescan::CreateParameters& params) override;
    cb::engine_errc continueRangeScan(
            CookieIface& cookie,
            const cb::rangescan::ContinueParameters& params) override;
    cb::engine_errc cancelRangeScan(CookieIface& cookie,
                                    Vbid vbid,
                                    cb::rangescan::Id uuid) override;
    std::pair<cb::engine_errc, nlohmann::json> getFusionStorageSnapshot(
            Vbid vbid,
            std::string_view snapshotUuid,
            std::time_t validity) override;
    cb::engine_errc releaseFusionStorageSnapshot(
            Vbid vbid, std::string_view snapshotUuid) override;
    std::pair<cb::engine_errc, std::vector<std::string>> mountVBucket(
            Vbid vbid, const std::vector<std::string>& paths) override;
    cb::engine_errc syncFusionLogstore(Vbid vbid) override;
    cb::engine_errc startFusionUploader(Vbid vbid, uint64_t term) override;
    cb::engine_errc stopFusionUploader(Vbid vbid) override;

    cb::engine_errc setChronicleAuthToken(std::string_view token) override;

    cb::engine_errc pause(folly::CancellationToken cancellationToken) override;
    cb::engine_errc resume() override;
    cb::engine_errc start_persistence(CookieIface& cookie) override;
    cb::engine_errc stop_persistence(CookieIface& cookie) override;
    cb::engine_errc wait_for_seqno_persistence(CookieIface& cookie,
                                               uint64_t seqno,
                                               Vbid vbid) override;
    cb::engine_errc set_active_encryption_keys(
            const nlohmann::json& json) override;
    cb::engine_errc prepare_snapshot(
            CookieIface& cookie,
            Vbid vbid,
            const std::function<void(const nlohmann::json&)>& callback)
            override;
    cb::engine_errc download_snapshot(CookieIface& cookie,
                                      Vbid vbid,
                                      std::string_view metadata) override;
    cb::engine_errc get_snapshot_file_info(
            CookieIface& cookie,
            std::string_view uuid,
            std::size_t file_id,
            const std::function<void(const nlohmann::json&)>& callback)
            override;
    cb::engine_errc release_snapshot(
            CookieIface& cookie, std::variant<Vbid, std::string_view>) override;

    ///////////////////////////////////////////////////////////////////////////
    //             All of the methods used in the DCP interface              //
    //                                                                       //
    // We don't support mocking with the DCP interface yet, so all access to //
    // the DCP interface will be proxied down to the underlying engine.      //
    ///////////////////////////////////////////////////////////////////////////
    cb::engine_errc step(CookieIface& cookie,
                         bool throttled,
                         DcpMessageProducersIface& producers) override;
    cb::engine_errc open(CookieIface& cookie,
                         uint32_t opaque,
                         uint32_t seqno,
                         cb::mcbp::DcpOpenFlag flags,
                         std::string_view name,
                         std::string_view value) override;
    cb::engine_errc add_stream(CookieIface& cookie,
                               uint32_t opaque,
                               Vbid vbucket,
                               cb::mcbp::DcpAddStreamFlag flags) override;
    cb::engine_errc close_stream(CookieIface& cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 cb::mcbp::DcpStreamId sid) override;
    cb::engine_errc stream_req(CookieIface& cookie,
                               cb::mcbp::DcpAddStreamFlag flags,
                               uint32_t opaque,
                               Vbid vbucket,
                               uint64_t start_seqno,
                               uint64_t end_seqno,
                               uint64_t vbucket_uuid,
                               uint64_t snap_start_seqno,
                               uint64_t snap_end_seqno,
                               uint64_t* rollback_seqno,
                               dcp_add_failover_log callback,
                               std::optional<std::string_view> json) override;
    cb::engine_errc get_failover_log(CookieIface& cookie,
                                     uint32_t opaque,
                                     Vbid vbucket,
                                     dcp_add_failover_log callback) override;
    cb::engine_errc stream_end(CookieIface& cookie,
                               uint32_t opaque,
                               Vbid vbucket,
                               cb::mcbp::DcpStreamEndStatus status) override;
    cb::engine_errc snapshot_marker(
            CookieIface& cookie,
            uint32_t opaque,
            Vbid vbucket,
            uint64_t start_seqno,
            uint64_t end_seqno,
            cb::mcbp::request::DcpSnapshotMarkerFlag flags,
            std::optional<uint64_t> high_completed_seqno,
            std::optional<uint64_t> high_prepared_seqno,
            std::optional<uint64_t> max_visible_seqno,
            std::optional<uint64_t> purge_seqno) override;
    cb::engine_errc mutation(CookieIface& cookie,
                             uint32_t opaque,
                             const DocKeyView& key,
                             cb::const_byte_buffer value,
                             uint8_t datatype,
                             uint64_t cas,
                             Vbid vbucket,
                             uint32_t flags,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             uint32_t expiration,
                             uint32_t lock_time,
                             cb::const_byte_buffer meta,
                             uint8_t nru) override;
    cb::engine_errc deletion(CookieIface& cookie,
                             uint32_t opaque,
                             const DocKeyView& key,
                             cb::const_byte_buffer value,
                             uint8_t datatype,
                             uint64_t cas,
                             Vbid vbucket,
                             uint64_t by_seqno,
                             uint64_t rev_seqno,
                             cb::const_byte_buffer meta) override;
    cb::engine_errc deletion_v2(CookieIface& cookie,
                                uint32_t opaque,
                                const DocKeyView& key,
                                cb::const_byte_buffer value,
                                uint8_t datatype,
                                uint64_t cas,
                                Vbid vbucket,
                                uint64_t by_seqno,
                                uint64_t rev_seqno,
                                uint32_t delete_time) override;
    cb::engine_errc expiration(CookieIface& cookie,
                               uint32_t opaque,
                               const DocKeyView& key,
                               cb::const_byte_buffer value,
                               uint8_t datatype,
                               uint64_t cas,
                               Vbid vbucket,
                               uint64_t by_seqno,
                               uint64_t rev_seqno,
                               uint32_t deleteTime) override;
    cb::engine_errc set_vbucket_state(CookieIface& cookie,
                                      uint32_t opaque,
                                      Vbid vbucket,
                                      vbucket_state_t state) override;
    cb::engine_errc noop(CookieIface& cookie, uint32_t opaque) override;
    cb::engine_errc buffer_acknowledgement(CookieIface& cookie,
                                           uint32_t opaque,
                                           uint32_t buffer_bytes) override;
    cb::engine_errc control(CookieIface& cookie,
                            uint32_t opaque,
                            std::string_view key,
                            std::string_view value) override;
    cb::engine_errc response_handler(
            CookieIface& cookie, const cb::mcbp::Response& response) override;
    cb::engine_errc system_event(CookieIface& cookie,
                                 uint32_t opaque,
                                 Vbid vbucket,
                                 mcbp::systemevent::id event,
                                 uint64_t bySeqno,
                                 mcbp::systemevent::version version,
                                 cb::const_byte_buffer key,
                                 cb::const_byte_buffer eventData) override;
    cb::engine_errc prepare(CookieIface& cookie,
                            uint32_t opaque,
                            const DocKeyView& key,
                            cb::const_byte_buffer value,
                            uint8_t datatype,
                            uint64_t cas,
                            Vbid vbucket,
                            uint32_t flags,
                            uint64_t by_seqno,
                            uint64_t rev_seqno,
                            uint32_t expiration,
                            uint32_t lock_time,
                            uint8_t nru,
                            DocumentState document_state,
                            cb::durability::Level level) override;
    cb::engine_errc seqno_acknowledged(CookieIface& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       uint64_t prepared_seqno) override;
    cb::engine_errc commit(CookieIface& cookie,
                           uint32_t opaque,
                           Vbid vbucket,
                           const DocKeyView& key,
                           uint64_t prepared_seqno,
                           uint64_t commit_seqno) override;
    cb::engine_errc abort(CookieIface& cookie,
                          uint32_t opaque,
                          Vbid vbucket,
                          const DocKeyView& key,
                          uint64_t prepared_seqno,
                          uint64_t abort_seqno) override;

protected:
    GET_SERVER_API gsa;
    ServerApi* real_api;

    // Actual engine we are proxying requests to.
    unique_engine_ptr real_engine;

    // Pointer to DcpIface for the underlying engine we are proxying; or
    // nullptr if it doesn't implement DcpIface;
    DcpIface* real_engine_dcp = nullptr;

    /**
     * Handle the control message for block monitor file
     *
     * @param cookie The cookie executing the operation
     * @param id The identifier used to represent the cookie
     * @param file The file to monitor
     * @param response callback used to send a response to the client
     * @return The standard engine error codes
     */
    cb::engine_errc handleBlockMonitorFile(CookieIface* cookie,
                                           uint32_t id,
                                           const std::string& file,
                                           const AddResponseFn& response);

    /**
     * Handle the control message for suspend
     *
     * @param cookie The cookie executing the operation
     * @param id The identifier used to represent the cookie to resume
     *           (the use of a different id is to allow resume to
     *           be sent on a different connection)
     * @param response callback used to send a response to the client
     * @return The standard engine error codes
     */
    cb::engine_errc handleSuspend(CookieIface* cookie,
                                  uint32_t id,
                                  const AddResponseFn& response);

    /**
     * Handle the control message for resume
     *
     * @param cookie The cookie executing the operation
     * @param id The identifier representing the connection to resume
     * @param response callback used to send a response to the client
     * @return The standard engine error codes
     */
    cb::engine_errc handleResume(CookieIface* cookie,
                                 uint32_t id,
                                 const AddResponseFn& response);

    /**
     * @param cookie the cookie executing the operation
     * @param key ID of the item whose CAS should be changed
     * @param cas The new CAS
     * @param response Response callback used to send a response to the client
     * @return Standard engine error codes
     */
    cb::engine_errc setItemCas(CookieIface* cookie,
                               const std::string& key,
                               uint32_t cas,
                               const AddResponseFn& response);

    cb::engine_errc checkLogLevels(CookieIface* cookie,
                                   uint32_t value,
                                   const AddResponseFn& response);

private:
    enum class Cmd {
        NONE,
        GET_INFO,
        ALLOCATE,
        REMOVE,
        GET,
        STORE,
        CAS,
        ARITHMETIC,
        LOCK,
        UNLOCK,
        FLUSH,
        GET_STATS,
        GET_META,
        UNKNOWN_COMMAND
    };

    const char* to_string(Cmd cmd);

    /**
     * Returns true if the next command should have a fake error code injected.
     * @param func Address of the command function (get, store, etc).
     * @param cookie The cookie for the user's request.
     * @param[out] Error code to return.
     */
    bool should_inject_error(Cmd cmd,
                             CookieIface* cookie,
                             cb::engine_errc& err);

    // Base class for all fault injection modes.
    struct FaultInjectMode {
        virtual ~FaultInjectMode() = default;

        explicit FaultInjectMode(cb::engine_errc injected_error_)
            : injected_error(injected_error_) {
        }

        // In the event of injecting an EWOULDBLOCK error, should the connection
        // be added to the pending_io_ops (and subsequently notified)?
        // @returns empty if shouldn't be added, otherwise contains the
        // status code to notify with.
        virtual std::optional<cb::engine_errc> add_to_pending_io_ops() {
            return cb::engine_errc::success;
        }

        virtual bool should_inject_error(Cmd cmd, cb::engine_errc& err) = 0;

        virtual std::string to_string() const = 0;

    protected:
        cb::engine_errc injected_error;
    };

    // Subclasses for each fault inject mode: /////////////////////////////////

    class ErrOnFirst : public FaultInjectMode {
    public:
        explicit ErrOnFirst(cb::engine_errc injected_error_)
            : FaultInjectMode(injected_error_) {
        }

        bool should_inject_error(Cmd cmd, cb::engine_errc& err) override {
            // Block unless the previous command from this cookie
            // was the same - i.e. all of a connections' commands
            // will EWOULDBLOCK the first time they are called.
            bool inject = (prev_cmd != cmd);
            prev_cmd = cmd;
            if (inject) {
                err = injected_error;
            }
            return inject;
        }

        std::string to_string() const override {
            return "ErrOnFirst inject_error=" + cb::to_string(injected_error);
        }

    private:
        // Last command issued by this cookie.
        Cmd prev_cmd = Cmd::NONE;
    };

    class ErrOnNextN : public FaultInjectMode {
    public:
        ErrOnNextN(cb::engine_errc injected_error_, uint32_t count_)
            : FaultInjectMode(injected_error_), count(count_) {
        }

        bool should_inject_error(Cmd cmd, cb::engine_errc& err) override {
            if (count > 0) {
                --count;
                err = injected_error;
                return true;
            }
            return false;
        }

        std::string to_string() const override {
            return std::string("ErrOnNextN") +
                   " inject_error=" + cb::to_string(injected_error) +
                   " count=" + std::to_string(count);
        }

    private:
        // The count of commands issued that should return error.
        uint32_t count;
    };

    class ErrRandom : public FaultInjectMode {
    public:
        ErrRandom(cb::engine_errc injected_error_, uint32_t percentage_)
            : FaultInjectMode(injected_error_), percentage_to_err(percentage_) {
        }

        bool should_inject_error(Cmd cmd, cb::engine_errc& err) override {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<uint32_t> dis(1, 100);
            if (dis(gen) < percentage_to_err) {
                err = injected_error;
                return true;
            }
            return false;
        }

        std::string to_string() const override {
            return std::string("ErrRandom") +
                   " inject_error=" + cb::to_string(injected_error) +
                   " percentage=" + std::to_string(percentage_to_err);
        }

    private:
        // Percentage chance that the specified error should be injected.
        uint32_t percentage_to_err;
    };

    /**
     * Injects a sequence of error codes for each call to should_inject_error().
     * If the end of the given sequence is reached, then throws
     * std::logic_error.
     *
     * cb::mcbp::Status::ReservedUserStart can be used to specify that the
     * no error is injected (the original status code is returned unchanged).
     */
    class ErrSequence : public FaultInjectMode {
    public:
        /**
         * Construct with a sequence of the specified error, or the 'normal'
         * status code.
         */
        ErrSequence(cb::engine_errc injected_error_, uint32_t sequence_)
            : FaultInjectMode(injected_error_) {
            for (int ii = 0; ii < 32; ii++) {
                if ((sequence_ & (1 << ii)) != 0) {
                    sequence.push_back(injected_error_);
                } else {
                    sequence.push_back(cb::engine_errc(-1));
                }
            }
            pos = sequence.begin();
        }

        /**
         * Construct with a specific sequence of (potentially different) status
         * codes encoded as vector of cb::engine_errc elements in the
         * request value.
         */
        explicit ErrSequence(std::vector<cb::engine_errc> sequence_)
            : FaultInjectMode(cb::engine_errc::success),
              sequence(std::move(sequence_)),
              pos(sequence.begin()) {
        }

        bool should_inject_error(Cmd cmd, cb::engine_errc& err) override {
            if (pos == sequence.end()) {
                throw std::logic_error(
                        "ErrSequence::should_inject_error() Reached end of "
                        "sequence");
            }
            bool inject = false;
            if (*pos != cb::engine_errc(-1)) {
                inject = true;
                err = *pos;
            }
            pos++;
            return inject;
        }

        std::optional<cb::engine_errc> add_to_pending_io_ops() override {
            // If this function has been called, should_inject_error() must
            // have returned true. Return the next status code in the sequnce
            // as the result of the pending IO.
            if (pos == sequence.end()) {
                throw std::logic_error(
                        "ErrSequence::add_to_pending_io_ops() Reached end of "
                        "sequence");
            }

            return *pos++;
        }

        std::string to_string() const override {
            std::stringstream ss;
            ss << "ErrSequence sequence=[";
            for (const auto& err : sequence) {
                if (err == cb::engine_errc(-1)) {
                    ss << "'<passthrough>',";
                } else {
                    ss << "'" << err << "',";
                }
            }
            ss << "] pos=" << pos - sequence.begin();
            return ss.str();
        }

    private:
        std::vector<cb::engine_errc> sequence;
        std::vector<cb::engine_errc>::const_iterator pos;
    };

    class ErrOnNoNotify : public FaultInjectMode {
        public:
            explicit ErrOnNoNotify(cb::engine_errc injected_error_)
                : FaultInjectMode(injected_error_) {
            }

            std::optional<cb::engine_errc> add_to_pending_io_ops() override {
                return {};
            }

            bool should_inject_error(Cmd cmd, cb::engine_errc& err) override {
                if (!issued_return_error) {
                    issued_return_error = true;
                    err = injected_error;
                    return true;
                }
                return false;
            }

            std::string to_string() const override {
                return std::string("ErrOnNoNotify") +
                       " inject_error=" + cb::to_string(injected_error) +
                       " issued_return_error=" +
                       std::to_string(issued_return_error);
            }

        private:
            // Record of whether have yet issued return error.
            bool issued_return_error = false;
        };

    class CASMismatch : public FaultInjectMode {
    public:
        explicit CASMismatch(uint32_t count_)
            : FaultInjectMode(cb::engine_errc::key_already_exists),
              count(count_) {
        }

        bool should_inject_error(Cmd cmd, cb::engine_errc& err) override {
            if (cmd == Cmd::CAS && (count > 0)) {
                --count;
                err = injected_error;
                return true;
            }
            return false;
        }

        std::string to_string() const override {
            return std::string("CASMismatch") +
                   " count=" + std::to_string(count);
        }

    protected:
        uint32_t count;
    };

    class SlowCasMismatch : public CASMismatch {
    public:
        explicit SlowCasMismatch(uint32_t count_)
            : CASMismatch(count_ & 0xffff),
              sleep_time((count_ & 0xffff0000) >> 16) {
            if (sleep_time.count() == 0) {
                sleep_time = std::chrono::milliseconds{1};
            }
        }

        bool should_inject_error(Cmd cmd, cb::engine_errc& err) override {
            if (CASMismatch::should_inject_error(cmd, err)) {
                std::this_thread::sleep_for(sleep_time);
                return true;
            }
            return false;
        }

        std::string to_string() const override {
            return fmt::format("SlowCasMismatch count={} sleep_time={}ms",
                               count,
                               sleep_time.count());
        }

    protected:
        std::chrono::milliseconds sleep_time;
    };

    // Map of connections (aka cookies) to their current mode.
    folly::Synchronized<
            std::unordered_map<
                    uint64_t,
                    std::pair<CookieIface*, std::shared_ptr<FaultInjectMode>>>,
            std::mutex>
            connection_map;

    /// A map from connection id to cookies which are suspended
    folly::Synchronized<std::unordered_map<uint32_t, CookieIface*>, std::mutex>
            suspended_map;

    bool suspendConn(CookieIface* cookie, uint32_t id) {
        return suspended_map.withLock([id, cookie](auto& map) {
            auto [iter, inserted] = map.insert({id, cookie});
            return inserted;
        });
    }

    bool resumeConn(uint32_t id) {
        CookieIface* cookie = nullptr;
        if (suspended_map.withLock([id, &cookie](auto& map) {
                auto iter = map.find(id);
                if (iter == map.cend()) {
                    return false;
                }
                cookie = iter->second;
                map.erase(iter);
                return true;
            })) {
            schedule_notification(cookie, cb::engine_errc::success);
            return true;
        }
        return false;
    }

    bool is_connection_suspended(CookieIface* cookie) {
        return suspended_map.withLock([cookie, this](const auto& map) {
            for (const auto& [id, c] : map) {
                if (c == cookie) {
                    return true;
                }
            }
            return false;
        });
    }

    void schedule_notification(CookieIface* cookie, cb::engine_errc status) {
        cookie->notifyIoComplete(status);
    }

    // Vector to keep track of the threads we've started to ensure
    // we don't leak memory ;-)
    folly::Synchronized<std::vector<std::unique_ptr<Couchbase::Thread>>,
                        std::mutex>
            threads;
};

EWB_Engine::EWB_Engine(GET_SERVER_API gsa_) : gsa(gsa_), real_api(gsa()) {
}

EWB_Engine::~EWB_Engine() {
    threads.lock()->clear();
}

void EWB_Engine::disconnect(CookieIface& cookie) {
    connection_map.lock()->erase(uint64_t(&cookie.getConnectionIface()));

    if (real_engine) {
        real_engine->disconnect(cookie);
    }
}

/* Returns true if the next command should have a fake error code injected.
 * @param func Address of the command function (get, store, etc).
 * @param cookie The cookie for the user's request.
 * @param[out] Error code to return.
 */
bool EWB_Engine::should_inject_error(Cmd cmd,
                                     CookieIface* cookie,
                                     cb::engine_errc& err) {
    if (is_connection_suspended(cookie)) {
        err = cb::engine_errc::would_block;
        return true;
    }

    return connection_map.withLock([&err, cmd, cookie, this](auto& map) {
        auto iter = map.find(uint64_t(&cookie->getConnectionIface()));
        if (iter == map.end()) {
            return false;
        }

        if (iter->second.first != cookie) {
            // The cookie is different so it represents a different command
            map.erase(iter);
            return false;
        }

        const bool inject = iter->second.second->should_inject_error(cmd, err);

        if (inject) {
            if (err == cb::engine_errc::would_block) {
                const auto add_to_pending_io_ops =
                        iter->second.second->add_to_pending_io_ops();
                if (add_to_pending_io_ops) {
                    // The server expects that if EWOULDBLOCK is returned
                    // then the server should be notified in the future when
                    // the operation is ready - so add this op to the
                    // pending IO queue.
                    schedule_notification(iter->second.first,
                                          *add_to_pending_io_ops);
                }
            }
        }

        return inject;
    });
}

cb::engine_errc EWB_Engine::initialize(std::string_view config_str,
                                       const nlohmann::json& encryption,
                                       std::string_view chronicleAuthToken) {
    std::string real_engine_name;
    auto engine_config = cb::config::filter(
            config_str, [&real_engine_name](auto k, auto v) -> bool {
                if (k == "ewb_real_engine") {
                    real_engine_name = v;
                    return false;
                }
                return true;
            });
    real_engine = createBucket(real_engine_name, gsa);

    if (!real_engine) {
        LOG_CRITICAL(
                "ERROR: EWB_Engine::initialize(): Failed create "
                "engine instance '{}'",
                real_engine_name);
        std::abort();
    }

    real_engine_dcp = dynamic_cast<DcpIface*>(real_engine.get());
    return real_engine->initialize(
            engine_config, encryption, chronicleAuthToken);
}

void EWB_Engine::destroy(bool force) {
    real_engine->destroy(force);
    (void)real_engine.release();
    delete this;
}

cb::engine_errc EWB_Engine::set_traffic_control_mode(CookieIface& cookie,
                                                     TrafficControlMode mode) {
    return real_engine->set_traffic_control_mode(cookie, mode);
}

cb::unique_item_ptr EWB_Engine::allocateItem(CookieIface& cookie,
                                             const DocKeyView& key,
                                             size_t nbytes,
                                             size_t priv_nbytes,
                                             uint32_t flags,
                                             rel_time_t exptime,
                                             uint8_t datatype,
                                             Vbid vbucket) {
    cb::engine_errc err = cb::engine_errc::success;
    if (should_inject_error(Cmd::ALLOCATE, &cookie, err)) {
        throw cb::engine_error(err, "ewb: injecting error");
    }
    return real_engine->allocateItem(cookie,
                                     key,
                                     nbytes,
                                     priv_nbytes,
                                     flags,
                                     exptime,
                                     datatype,
                                     vbucket);
}

cb::engine_errc EWB_Engine::remove(
        CookieIface& cookie,
        const DocKeyView& key,
        uint64_t& cas,
        Vbid vbucket,
        const std::optional<cb::durability::Requirements>& durability,
        mutation_descr_t& mut_info) {
    cb::engine_errc err = cb::engine_errc::success;
    if (should_inject_error(Cmd::REMOVE, &cookie, err)) {
        return err;
    }
    return real_engine->remove(cookie, key, cas, vbucket, durability, mut_info);
}

void EWB_Engine::release(ItemIface& item) {
    real_engine->release(item);
}

cb::EngineErrorItemPair EWB_Engine::get(CookieIface& cookie,
                                        const DocKeyView& key,
                                        Vbid vbucket,
                                        DocStateFilter documentStateFilter) {
    cb::engine_errc err = cb::engine_errc::success;
    if (should_inject_error(Cmd::GET, &cookie, err)) {
        return std::make_pair(
                err, cb::unique_item_ptr{nullptr, cb::ItemDeleter{this}});
    }
    return real_engine->get(cookie, key, vbucket, documentStateFilter);
}

cb::EngineErrorItemPair EWB_Engine::get_random_document(CookieIface& cookie,
                                                        CollectionID cid) {
    cb::engine_errc err;
    if (should_inject_error(Cmd::GET, &cookie, err)) {
        return std::make_pair(
                err, cb::unique_item_ptr{nullptr, cb::ItemDeleter{this}});
    }
    return real_engine->get_random_document(cookie, cid);
}

cb::EngineErrorItemPair EWB_Engine::get_if(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const std::function<bool(const item_info&)>& filter) {
    cb::engine_errc err = cb::engine_errc::success;
    if (should_inject_error(Cmd::GET, &cookie, err)) {
        return cb::makeEngineErrorItemPair(cb::engine_errc::would_block);
    }
    return real_engine->get_if(cookie, key, vbucket, filter);
}

cb::EngineErrorItemPair EWB_Engine::get_and_touch(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        uint32_t exptime,
        const std::optional<cb::durability::Requirements>& durability) {
    cb::engine_errc err = cb::engine_errc::success;
    if (should_inject_error(Cmd::GET, &cookie, err)) {
        return cb::makeEngineErrorItemPair(cb::engine_errc::would_block);
    }
    return real_engine->get_and_touch(
            cookie, key, vbucket, exptime, durability);
}

cb::EngineErrorItemPair EWB_Engine::get_locked(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        std::chrono::seconds lock_timeout) {
    cb::engine_errc err = cb::engine_errc::success;
    if (should_inject_error(Cmd::LOCK, &cookie, err)) {
        return cb::makeEngineErrorItemPair(err);
    }
    return real_engine->get_locked(cookie, key, vbucket, lock_timeout);
}

cb::engine_errc EWB_Engine::unlock(CookieIface& cookie,
                                   const DocKeyView& key,
                                   Vbid vbucket,
                                   uint64_t cas) {
    cb::engine_errc err = cb::engine_errc::success;
    if (should_inject_error(Cmd::UNLOCK, &cookie, err)) {
        return err;
    }
    return real_engine->unlock(cookie, key, vbucket, cas);
}

cb::EngineErrorMetadataPair EWB_Engine::get_meta(CookieIface& cookie,
                                                 const DocKeyView& key,
                                                 Vbid vbucket) {
    cb::engine_errc err = cb::engine_errc::success;
    if (should_inject_error(Cmd::GET_META, &cookie, err)) {
        return std::make_pair(err, item_info());
    }
    return real_engine->get_meta(cookie, key, vbucket);
}

cb::engine_errc EWB_Engine::evict_key(CookieIface& cookie,
                                      const DocKeyView& key,
                                      Vbid vbucket) {
    return real_engine->evict_key(cookie, key, vbucket);
}

cb::engine_errc EWB_Engine::observe(
        CookieIface& cookie,
        const DocKeyView& key,
        Vbid vbucket,
        const std::function<void(uint8_t, uint64_t)>& key_handler,
        uint64_t& persist_time_hint) {
    return real_engine->observe(
            cookie, key, vbucket, key_handler, persist_time_hint);
}

cb::engine_errc EWB_Engine::store(
        CookieIface& cookie,
        ItemIface& item,
        uint64_t& cas,
        StoreSemantics operation,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl) {
    cb::engine_errc err = cb::engine_errc::success;
    Cmd opcode = (operation == StoreSemantics::CAS) ? Cmd::CAS : Cmd::STORE;
    if (should_inject_error(opcode, &cookie, err)) {
        return err;
    }
    return real_engine->store(cookie,
                              item,
                              cas,
                              operation,
                              durability,
                              document_state,
                              preserveTtl);
}

cb::EngineErrorCasPair EWB_Engine::store_if(
        CookieIface& cookie,
        ItemIface& item,
        uint64_t cas,
        StoreSemantics operation,
        const cb::StoreIfPredicate& predicate,
        const std::optional<cb::durability::Requirements>& durability,
        DocumentState document_state,
        bool preserveTtl) {
    cb::engine_errc err = cb::engine_errc::success;
    Cmd opcode = (operation == StoreSemantics::CAS) ? Cmd::CAS : Cmd::STORE;
    if (should_inject_error(opcode, &cookie, err)) {
        return {err, 0};
    }
    return real_engine->store_if(cookie,
                                 item,
                                 cas,
                                 operation,
                                 predicate,
                                 durability,
                                 document_state,
                                 preserveTtl);
}

cb::engine_errc EWB_Engine::flush(CookieIface& cookie) {
    // Flush is a little different - it often returns EWOULDBLOCK, and
    // notify_io_complete() just tells the server it can issue it's *next*
    // command (i.e. no need to re-flush). Therefore just pass Flush
    // straight through for now.
    return real_engine->flush(cookie);
}

cb::engine_errc EWB_Engine::get_stats(CookieIface& cookie,
                                      std::string_view key,
                                      std::string_view value,
                                      const AddStatFn& add_stat,
                                      const CheckYieldFn& check_yield) {
    auto err = cb::engine_errc::success;
    if (should_inject_error(Cmd::GET_STATS, &cookie, err)) {
        return err;
    }
    return real_engine->get_stats(cookie, key, value, add_stat, check_yield);
}

void EWB_Engine::reset_stats(CookieIface& cookie) {
    real_engine->reset_stats(cookie);
}

/**
 * Handle 'unknown_command'. In additional to wrapping calls to the
 * underlying real engine, this is also used to configure
 * ewouldblock_engine itself using he CMD_EWOULDBLOCK_CTL opcode.
 */
cb::engine_errc EWB_Engine::unknown_command(CookieIface& cookie,
                                            const cb::mcbp::Request& req,
                                            const AddResponseFn& response) {
    const auto opcode = req.getClientOpcode();
    if (opcode == cb::mcbp::ClientOpcode::EwouldblockCtl) {
        using cb::mcbp::request::EWB_Payload;
        const auto& payload = req.getCommandSpecifics<EWB_Payload>();
        const auto mode = static_cast<EWBEngineMode>(payload.getMode());
        const auto value = payload.getValue();
        const auto injected_error =
                static_cast<cb::engine_errc>(payload.getInjectError());
        const std::string key(req.getKeyString());
        std::shared_ptr<FaultInjectMode> new_mode = nullptr;

        // Validate mode, and construct new fault injector.
        switch (mode) {
        case EWBEngineMode::Next_N:
            new_mode = std::make_shared<ErrOnNextN>(injected_error, value);
            break;

        case EWBEngineMode::Random:
            new_mode = std::make_shared<ErrRandom>(injected_error, value);
            break;

        case EWBEngineMode::First:
            new_mode = std::make_shared<ErrOnFirst>(injected_error);
            break;

        case EWBEngineMode::Sequence: {
            std::vector<cb::engine_errc> decoded;
            for (unsigned int ii = 0; ii < key.size() / sizeof(cb::engine_errc);
                 ii++) {
                auto status = *reinterpret_cast<const uint32_t*>(
                        key.data() + (ii * sizeof(cb::engine_errc)));
                status = ntohl(status);
                decoded.emplace_back(cb::engine_errc(status));
            }
            new_mode = std::make_shared<ErrSequence>(decoded);
            break;
        }

        case EWBEngineMode::No_Notify:
            new_mode = std::make_shared<ErrOnNoNotify>(injected_error);
            break;

        case EWBEngineMode::CasMismatch:
            new_mode = std::make_shared<CASMismatch>(value);
            break;

        case EWBEngineMode::BlockMonitorFile:
            return handleBlockMonitorFile(&cookie, value, key, response);

        case EWBEngineMode::Suspend:
            return handleSuspend(&cookie, value, response);

        case EWBEngineMode::Resume:
            return handleResume(&cookie, value, response);

        case EWBEngineMode::SetItemCas:
            return setItemCas(&cookie, key, value, response);

        case EWBEngineMode::CheckLogLevels:
            return checkLogLevels(&cookie, value, response);

        case EWBEngineMode::ThrowException:
            // Reserve the cookie and schedule a release of the
            // cookie and throw an exception for the cookie
            {
                cookie.reserve();
                std::thread release{[&cookie]() {
                    // This will block on the thread mutex
                    cookie.release();
                }};
                release.detach();
            }
            throw std::runtime_error(
                    "EWB::unknown_command: you told me to throw an "
                    "exception");

        case EWBEngineMode::SlowCasMismatch:
            new_mode = std::make_shared<SlowCasMismatch>(value);
            break;
        }

        if (new_mode == nullptr) {
            LOG_WARNING_CTX(
                    "EWB_Engine::unknown_command(): "
                    "Unexpected mode for EWOULDBLOCK_CTL, ",
                    {"mode", (unsigned int)mode});
            response({},
                     {},
                     {},
                     ValueIsJson::No,
                     cb::mcbp::Status::Einval,
                     /*cas*/ 0,
                     cookie);
            return cb::engine_errc::failed;
        }
        try {
            connection_map.withLock([&cookie, new_mode](auto& map) {
                map[uint64_t(&cookie.getConnectionIface())] = {&cookie,
                                                               new_mode};
            });

            response({},
                     {},
                     {},
                     ValueIsJson::No,
                     cb::mcbp::Status::Success,
                     /*cas*/ 0,
                     cookie);
            return cb::engine_errc::success;
        } catch (std::bad_alloc&) {
            return cb::engine_errc::no_memory;
        }
    }
    auto err = cb::engine_errc::success;
    if (should_inject_error(Cmd::UNKNOWN_COMMAND, &cookie, err)) {
        return err;
    }
    return real_engine->unknown_command(cookie, req, response);
}

bool EWB_Engine::get_item_info(const ItemIface& item, item_info& item_info) {
    return real_engine->get_item_info(item, item_info);
}

cb::engine::FeatureSet EWB_Engine::getFeatures() {
    return real_engine->getFeatures();
}

bool EWB_Engine::isXattrEnabled() {
    return real_engine->isXattrEnabled();
}

std::optional<cb::HlcTime> EWB_Engine::getVBucketHlcNow(Vbid vbucket) {
    return real_engine->getVBucketHlcNow(vbucket);
}

BucketCompressionMode EWB_Engine::getCompressionMode() {
    return real_engine->getCompressionMode();
}

size_t EWB_Engine::getMaxItemSize() {
    return real_engine->getMaxItemSize();
}

float EWB_Engine::getMinCompressionRatio() {
    return real_engine->getMinCompressionRatio();
}

cb::engine_errc EWB_Engine::setParameter(CookieIface& cookie,
                                         EngineParamCategory category,
                                         std::string_view key,
                                         std::string_view value,
                                         Vbid vbucket) {
    return real_engine->setParameter(cookie, category, key, value, vbucket);
}

cb::engine_errc EWB_Engine::compactDatabase(
        CookieIface& cookie,
        Vbid vbid,
        uint64_t purge_before_ts,
        uint64_t purge_before_seq,
        bool drop_deletes,
        const std::vector<std::string>& obsolete_keys) {
    return real_engine->compactDatabase(cookie,
                                        vbid,
                                        purge_before_ts,
                                        purge_before_seq,
                                        drop_deletes,
                                        obsolete_keys);
}

std::pair<cb::engine_errc, vbucket_state_t> EWB_Engine::getVBucket(
        CookieIface& cookie, Vbid vbid) {
    return real_engine->getVBucket(cookie, vbid);
}

cb::engine_errc EWB_Engine::setVBucket(CookieIface& cookie,
                                       Vbid vbid,
                                       uint64_t cas,
                                       vbucket_state_t state,
                                       nlohmann::json* meta) {
    return real_engine->setVBucket(cookie, vbid, cas, state, meta);
}

cb::engine_errc EWB_Engine::deleteVBucket(CookieIface& cookie,
                                          Vbid vbid,
                                          bool sync) {
    return real_engine->deleteVBucket(cookie, vbid, sync);
}

std::pair<cb::engine_errc, cb::rangescan::Id> EWB_Engine::createRangeScan(
        CookieIface& cookie, const cb::rangescan::CreateParameters& params) {
    return real_engine->createRangeScan(cookie, params);
}

cb::engine_errc EWB_Engine::continueRangeScan(
        CookieIface& cookie, const cb::rangescan::ContinueParameters& params) {
    return real_engine->continueRangeScan(cookie, params);
}

cb::engine_errc EWB_Engine::cancelRangeScan(CookieIface& cookie,
                                            Vbid vbid,
                                            cb::rangescan::Id uuid) {
    return real_engine->cancelRangeScan(cookie, vbid, uuid);
}

std::pair<cb::engine_errc, nlohmann::json> EWB_Engine::getFusionStorageSnapshot(
        Vbid vbid, std::string_view snapshotUuid, std::time_t validity) {
    return real_engine->getFusionStorageSnapshot(vbid, snapshotUuid, validity);
}

cb::engine_errc EWB_Engine::releaseFusionStorageSnapshot(
        Vbid vbid, std::string_view snapshotUuid) {
    return real_engine->releaseFusionStorageSnapshot(vbid, snapshotUuid);
}

std::pair<cb::engine_errc, std::vector<std::string>> EWB_Engine::mountVBucket(
        Vbid vbid, const std::vector<std::string>& paths) {
    return real_engine->mountVBucket(vbid, paths);
}

cb::engine_errc EWB_Engine::syncFusionLogstore(Vbid vbid) {
    return real_engine->syncFusionLogstore(vbid);
}

cb::engine_errc EWB_Engine::startFusionUploader(Vbid vbid, uint64_t term) {
    return real_engine->startFusionUploader(vbid, term);
}

cb::engine_errc EWB_Engine::stopFusionUploader(Vbid vbid) {
    return real_engine->stopFusionUploader(vbid);
}

cb::engine_errc EWB_Engine::setChronicleAuthToken(std::string_view token) {
    return real_engine->setChronicleAuthToken(token);
}

cb::engine_errc EWB_Engine::pause(folly::CancellationToken cancellationToken) {
    return real_engine->pause(cancellationToken);
}

cb::engine_errc EWB_Engine::resume() {
    return real_engine->resume();
}

cb::engine_errc EWB_Engine::start_persistence(CookieIface& cookie) {
    return real_engine->start_persistence(cookie);
}

cb::engine_errc EWB_Engine::stop_persistence(CookieIface& cookie) {
    return real_engine->stop_persistence(cookie);
}

cb::engine_errc EWB_Engine::wait_for_seqno_persistence(CookieIface& cookie,
                                                       uint64_t seqno,
                                                       Vbid vbid) {
    return real_engine->wait_for_seqno_persistence(cookie, seqno, vbid);
}

cb::engine_errc EWB_Engine::set_active_encryption_keys(
        const nlohmann::json& json) {
    return real_engine->set_active_encryption_keys(json);
}

cb::engine_errc EWB_Engine::prepare_snapshot(
        CookieIface& cookie,
        Vbid vbid,
        const std::function<void(const nlohmann::json&)>& callback) {
    return real_engine->prepare_snapshot(cookie, vbid, callback);
}
cb::engine_errc EWB_Engine::download_snapshot(CookieIface& cookie,
                                              Vbid vbid,
                                              std::string_view metadata) {
    return real_engine->download_snapshot(cookie, vbid, metadata);
}
cb::engine_errc EWB_Engine::get_snapshot_file_info(
        CookieIface& cookie,
        std::string_view uuid,
        std::size_t file_id,
        const std::function<void(const nlohmann::json&)>& callback) {
    return real_engine->get_snapshot_file_info(cookie, uuid, file_id, callback);
}
cb::engine_errc EWB_Engine::release_snapshot(
        CookieIface& cookie, std::variant<Vbid, std::string_view> snap) {
    return real_engine->release_snapshot(cookie, snap);
}

cb::engine_errc EWB_Engine::step(CookieIface& cookie,
                                 bool throttled,
                                 DcpMessageProducersIface& producers) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->step(cookie, throttled, producers);
}

cb::engine_errc EWB_Engine::open(CookieIface& cookie,
                                 uint32_t opaque,
                                 uint32_t seqno,
                                 cb::mcbp::DcpOpenFlag flags,
                                 std::string_view name,
                                 std::string_view value) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->open(cookie, opaque, seqno, flags, name, value);
}

cb::engine_errc EWB_Engine::stream_req(CookieIface& cookie,
                                       cb::mcbp::DcpAddStreamFlag flags,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       uint64_t start_seqno,
                                       uint64_t end_seqno,
                                       uint64_t vbucket_uuid,
                                       uint64_t snap_start_seqno,
                                       uint64_t snap_end_seqno,
                                       uint64_t* rollback_seqno,
                                       dcp_add_failover_log callback,
                                       std::optional<std::string_view> json) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->stream_req(cookie,
                                       flags,
                                       opaque,
                                       vbucket,
                                       start_seqno,
                                       end_seqno,
                                       vbucket_uuid,
                                       snap_start_seqno,
                                       snap_end_seqno,
                                       rollback_seqno,
                                       callback,
                                       json);
}

cb::engine_errc EWB_Engine::add_stream(CookieIface& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       cb::mcbp::DcpAddStreamFlag flags) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->add_stream(cookie, opaque, vbucket, flags);
}

cb::engine_errc EWB_Engine::close_stream(CookieIface& cookie,
                                         uint32_t opaque,
                                         Vbid vbucket,
                                         cb::mcbp::DcpStreamId sid) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->close_stream(cookie, opaque, vbucket, sid);
}

cb::engine_errc EWB_Engine::get_failover_log(CookieIface& cookie,
                                             uint32_t opaque,
                                             Vbid vbucket,
                                             dcp_add_failover_log callback) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->get_failover_log(cookie, opaque, vbucket, callback);
}

cb::engine_errc EWB_Engine::stream_end(CookieIface& cookie,
                                       uint32_t opaque,
                                       Vbid vbucket,
                                       cb::mcbp::DcpStreamEndStatus status) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->stream_end(cookie, opaque, vbucket, status);
}

cb::engine_errc EWB_Engine::snapshot_marker(
        CookieIface& cookie,
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        cb::mcbp::request::DcpSnapshotMarkerFlag flags,
        std::optional<uint64_t> high_completed_seqno,
        std::optional<uint64_t> high_prepared_seqno,
        std::optional<uint64_t> max_visible_seqno,
        std::optional<uint64_t> purge_seqno) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->snapshot_marker(cookie,
                                            opaque,
                                            vbucket,
                                            start_seqno,
                                            end_seqno,
                                            flags,
                                            high_completed_seqno,
                                            high_prepared_seqno,
                                            max_visible_seqno,
                                            purge_seqno);
}

cb::engine_errc EWB_Engine::mutation(CookieIface& cookie,
                                     uint32_t opaque,
                                     const DocKeyView& key,
                                     cb::const_byte_buffer value,
                                     uint8_t datatype,
                                     uint64_t cas,
                                     Vbid vbucket,
                                     uint32_t flags,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     uint32_t expiration,
                                     uint32_t lock_time,
                                     cb::const_byte_buffer meta,
                                     uint8_t nru) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->mutation(cookie,
                                     opaque,
                                     key,
                                     value,
                                     datatype,
                                     cas,
                                     vbucket,
                                     flags,
                                     by_seqno,
                                     rev_seqno,
                                     expiration,
                                     lock_time,
                                     meta,
                                     nru);
}

cb::engine_errc EWB_Engine::deletion(CookieIface& cookie,
                                     uint32_t opaque,
                                     const DocKeyView& key,
                                     cb::const_byte_buffer value,
                                     uint8_t datatype,
                                     uint64_t cas,
                                     Vbid vbucket,
                                     uint64_t by_seqno,
                                     uint64_t rev_seqno,
                                     cb::const_byte_buffer meta) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->deletion(cookie,
                                     opaque,
                                     key,
                                     value,
                                     datatype,
                                     cas,
                                     vbucket,
                                     by_seqno,
                                     rev_seqno,
                                     meta);
}

cb::engine_errc EWB_Engine::deletion_v2(CookieIface& cookie,
                                        uint32_t opaque,
                                        const DocKeyView& key,
                                        cb::const_byte_buffer value,
                                        uint8_t datatype,
                                        uint64_t cas,
                                        Vbid vbucket,
                                        uint64_t by_seqno,
                                        uint64_t rev_seqno,
                                        uint32_t delete_time) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->deletion_v2(cookie,
                                        opaque,
                                        key,
                                        value,
                                        datatype,
                                        cas,
                                        vbucket,
                                        by_seqno,
                                        rev_seqno,
                                        delete_time);
}

cb::engine_errc EWB_Engine::expiration(CookieIface& cookie,
                                       uint32_t opaque,
                                       const DocKeyView& key,
                                       cb::const_byte_buffer value,
                                       uint8_t datatype,
                                       uint64_t cas,
                                       Vbid vbucket,
                                       uint64_t by_seqno,
                                       uint64_t rev_seqno,
                                       uint32_t deleteTime) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->expiration(cookie,
                                       opaque,
                                       key,
                                       value,
                                       datatype,
                                       cas,
                                       vbucket,
                                       by_seqno,
                                       rev_seqno,
                                       deleteTime);
}

cb::engine_errc EWB_Engine::set_vbucket_state(CookieIface& cookie,
                                              uint32_t opaque,
                                              Vbid vbucket,
                                              vbucket_state_t state) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->set_vbucket_state(cookie, opaque, vbucket, state);
}

cb::engine_errc EWB_Engine::noop(CookieIface& cookie, uint32_t opaque) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->noop(cookie, opaque);
}

cb::engine_errc EWB_Engine::buffer_acknowledgement(CookieIface& cookie,
                                                   uint32_t opaque,
                                                   uint32_t buffer_bytes) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->buffer_acknowledgement(
            cookie, opaque, buffer_bytes);
}

cb::engine_errc EWB_Engine::control(CookieIface& cookie,
                                    uint32_t opaque,
                                    std::string_view key,
                                    std::string_view value) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->control(cookie, opaque, key, value);
}

cb::engine_errc EWB_Engine::response_handler(
        CookieIface& cookie, const cb::mcbp::Response& response) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->response_handler(cookie, response);
}

cb::engine_errc EWB_Engine::system_event(CookieIface& cookie,
                                         uint32_t opaque,
                                         Vbid vbucket,
                                         mcbp::systemevent::id event,
                                         uint64_t bySeqno,
                                         mcbp::systemevent::version version,
                                         cb::const_byte_buffer key,
                                         cb::const_byte_buffer eventData) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->system_event(
            cookie, opaque, vbucket, event, bySeqno, version, key, eventData);
}

cb::engine_errc EWB_Engine::prepare(CookieIface& cookie,
                                    uint32_t opaque,
                                    const DocKeyView& key,
                                    cb::const_byte_buffer value,
                                    uint8_t datatype,
                                    uint64_t cas,
                                    Vbid vbucket,
                                    uint32_t flags,
                                    uint64_t by_seqno,
                                    uint64_t rev_seqno,
                                    uint32_t expiration,
                                    uint32_t lock_time,
                                    uint8_t nru,
                                    DocumentState document_state,
                                    cb::durability::Level level) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->prepare(cookie,
                                    opaque,
                                    key,
                                    value,
                                    datatype,
                                    cas,
                                    vbucket,
                                    flags,
                                    by_seqno,
                                    rev_seqno,
                                    expiration,
                                    lock_time,
                                    nru,
                                    document_state,
                                    level);
}
cb::engine_errc EWB_Engine::seqno_acknowledged(CookieIface& cookie,
                                               uint32_t opaque,
                                               Vbid vbucket,
                                               uint64_t prepared_seqno) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->seqno_acknowledged(
            cookie, opaque, vbucket, prepared_seqno);
}
cb::engine_errc EWB_Engine::commit(CookieIface& cookie,
                                   uint32_t opaque,
                                   Vbid vbucket,
                                   const DocKeyView& key,
                                   uint64_t prepared_seqno,
                                   uint64_t commit_seqno) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->commit(
            cookie, opaque, vbucket, key, prepared_seqno, commit_seqno);
}

cb::engine_errc EWB_Engine::abort(CookieIface& cookie,
                                  uint32_t opaque,
                                  Vbid vbucket,
                                  const DocKeyView& key,
                                  uint64_t prepared_seqno,
                                  uint64_t abort_seqno) {
    if (!real_engine_dcp) {
        return cb::engine_errc::not_supported;
    }
    return real_engine_dcp->abort(
            cookie, opaque, vbucket, key, prepared_seqno, abort_seqno);
}

unique_engine_ptr create_ewouldblock_instance(GET_SERVER_API gsa) {
    return unique_engine_ptr{new EWB_Engine(gsa)};
}

const char* EWB_Engine::to_string(const Cmd cmd) {
    switch (cmd) {
    case Cmd::NONE:
        return "NONE";
    case Cmd::GET_INFO:
        return "GET_INFO";
    case Cmd::GET_META:
        return "GET_META";
    case Cmd::ALLOCATE:
        return "ALLOCATE";
    case Cmd::REMOVE:
        return "REMOVE";
    case Cmd::GET:
        return "GET";
    case Cmd::STORE:
        return "STORE";
    case Cmd::CAS:
        return "CAS";
    case Cmd::ARITHMETIC:
        return "ARITHMETIC";
    case Cmd::FLUSH:
        return "FLUSH";
    case Cmd::GET_STATS:
        return "GET_STATS";
    case Cmd::UNKNOWN_COMMAND:
        return "UNKNOWN_COMMAND";
    case Cmd::LOCK:
        return "LOCK";
    case Cmd::UNLOCK:
        return "UNLOCK";
    }
    throw std::invalid_argument("EWB_Engine::to_string() Unknown command");
}

cb::engine_errc EWB_Engine::handleBlockMonitorFile(
        CookieIface* cookie,
        uint32_t id,
        const std::string& file,
        const AddResponseFn& response) {
    if (file.empty()) {
        return cb::engine_errc::invalid_arguments;
    }

    if (!cb::io::isFile(file)) {
        return cb::engine_errc::no_such_key;
    }

    if (!suspendConn(cookie, id)) {
        LOG_WARNING_CTX(
                "EWB_Engine::handleBlockMonitorFile(): Id already registered",
                {"conn_id", id});
        return cb::engine_errc::key_already_exists;
    }

    try {
        /**
         * The BlockMonitorThread represents the thread that is
         * monitoring the "lock" file. Once the file is no longer
         * there it will call the provided callback function.
         */
        class BlockMonitorThread : public Couchbase::Thread {
        public:
            BlockMonitorThread(std::string file_,
                               std::function<void()> callback)
                : Thread("ewb:BlockMon"),
                  file(std::move(file_)),
                  callback(std::move(callback)) {
            }

            /**
             * Wait for the underlying thread to reach the zombie state
             * (== terminated, but not reaped)
             */
            ~BlockMonitorThread() override {
                waitForState(Couchbase::ThreadState::Zombie);
            }

        protected:
            void run() override {
                setRunning();

                // @todo Use the file monitoring APIs to avoid this "busy" loop
                while (cb::io::isFile(file)) {
                    std::this_thread::sleep_for(std::chrono::microseconds(100));
                }

                callback();
            }

        private:
            const std::string file;
            std::function<void()> callback;
        };

        std::unique_ptr<Couchbase::Thread> thread(
                new BlockMonitorThread(file, [this, id]() { resumeConn(id); }));
        thread->start();
        threads.lock()->emplace_back(thread.release());
    } catch (std::exception& e) {
        LOG_WARNING_CTX(
                "EWB_Engine::handleBlockMonitorFile(): Failed to create "
                "block monitor thread",
                {"error", e.what()});
        return cb::engine_errc::failed;
    }

    response({},
             {},
             {},
             ValueIsJson::No,
             cb::mcbp::Status::Success,
             /*cas*/ 0,
             *cookie);
    return cb::engine_errc::success;
}

cb::engine_errc EWB_Engine::handleSuspend(CookieIface* cookie,
                                          uint32_t id,
                                          const AddResponseFn& response) {
    if (suspendConn(cookie, id)) {
        response({},
                 {},
                 {},
                 ValueIsJson::No,
                 cb::mcbp::Status::Success,
                 /*cas*/ 0,
                 *cookie);
        return cb::engine_errc::success;
    }
    LOG_WARNING_CTX("EWB_Engine::handleSuspend(): Id already registered",
                    {"conn_id", id});
    return cb::engine_errc::key_already_exists;
}

cb::engine_errc EWB_Engine::handleResume(CookieIface* cookie,
                                         uint32_t id,
                                         const AddResponseFn& response) {
    if (resumeConn(id)) {
        response({},
                 {},
                 {},
                 ValueIsJson::No,
                 cb::mcbp::Status::Success,
                 /*cas*/ 0,
                 *cookie);
        return cb::engine_errc::success;
    }
    LOG_WARNING_CTX(
            "EWB_Engine::unknown_command(): No connection registered with "
            "requested id",
            {"conn_id", id});
    return cb::engine_errc::invalid_arguments;
}

cb::engine_errc EWB_Engine::setItemCas(CookieIface* cookie,
                                       const std::string& key,
                                       uint32_t cas,
                                       const AddResponseFn& response) {
    uint64_t cas64 = cas;
    if (cas == static_cast<uint32_t>(-1)) {
        cas64 = LOCKED_CAS;
    }

    auto rv = real_engine->get(*cookie,
                               DocKeyView{key, DocKeyEncodesCollectionId::No},
                               Vbid(0),
                               DocStateFilter::Alive);
    if (rv.first != cb::engine_errc::success) {
        return rv.first;
    }

    rv.second->setCas(cas64);
    response(
            {}, {}, {}, ValueIsJson::No, cb::mcbp::Status::Success, 0, *cookie);
    return cb::engine_errc::success;
}

cb::engine_errc EWB_Engine::checkLogLevels(CookieIface* cookie,
                                           uint32_t value,
                                           const AddResponseFn& response) {
    auto level = spdlog::level::level_enum(value);
    auto rsp = cb::logger::checkLogLevels(level);

    response({},
             {},
             {},
             ValueIsJson::No,
             rsp ? cb::mcbp::Status::Success : cb::mcbp::Status::Einval,
             0,
             *cookie);
    return cb::engine_errc::success;
}

cb::engine_errc EWB_Engine::set_collection_manifest(CookieIface& cookie,
                                                    std::string_view json) {
    return real_engine->set_collection_manifest(cookie, json);
}

cb::engine_errc EWB_Engine::get_collection_manifest(
        CookieIface& cookie, const AddResponseFn& response) {
    return real_engine->get_collection_manifest(cookie, response);
}

cb::EngineErrorGetCollectionIDResult EWB_Engine::get_collection_id(
        CookieIface& cookie, std::string_view path) {
    return real_engine->get_collection_id(cookie, path);
}

cb::EngineErrorGetScopeIDResult EWB_Engine::get_scope_id(
        CookieIface& cookie, std::string_view path) {
    return real_engine->get_scope_id(cookie, path);
}

void EWB_Engine::initiate_shutdown() {
    if (real_engine) {
        real_engine->initiate_shutdown();
    }
}

cb::EngineErrorGetCollectionMetaResult EWB_Engine::get_collection_meta(
        CookieIface& cookie, CollectionID cid, std::optional<Vbid> vbid) const {
    return real_engine->get_collection_meta(cookie, cid, std::optional<Vbid>());
}
