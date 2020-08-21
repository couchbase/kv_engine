/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2020 Couchbase, Inc
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

#include "collections_dcp_producers.h"

#include "collections/collections_types.h"
#include "item.h"
#include "tests/mock/mock_dcp_consumer.h"

#include "folly/portability/GTest.h"

/*
 * DCP callback method to push SystemEvents on to the consumer
 */
ENGINE_ERROR_CODE CollectionsDcpTestProducers::system_event(
        uint32_t opaque,
        Vbid vbucket,
        mcbp::systemevent::id event,
        uint64_t bySeqno,
        mcbp::systemevent::version version,
        cb::const_byte_buffer key,
        cb::const_byte_buffer eventData,
        cb::mcbp::DcpStreamId sid) {
    (void)vbucket; // ignored as we are connecting VBn to VBn+1
    clear_dcp_data();
    last_op = cb::mcbp::ClientOpcode::DcpSystemEvent;
    last_system_event = event;
    // Validate the provided parameters
    cb::mcbp::request::DcpSystemEventPayload extras(bySeqno, event, version);
    EXPECT_TRUE(extras.isValidVersion());
    EXPECT_TRUE(extras.isValidEvent());
    last_system_event_data.insert(
            last_system_event_data.begin(), eventData.begin(), eventData.end());
    last_system_event_version = version;
    last_stream_id = sid;

    switch (event) {
    case mcbp::systemevent::id::CreateCollection: {
        last_collection_id =
                reinterpret_cast<const Collections::CreateEventDcpData*>(
                        eventData.data())
                        ->cid.to_host();
        if (version == mcbp::systemevent::version::version0) {
            // Using the ::size directly in the EXPECT is failing to link on
            // OSX build, but copying the value works.
            const auto expectedSize = Collections::CreateEventDcpData::size;
            EXPECT_EQ(expectedSize, eventData.size());
            const auto* ev =
                    reinterpret_cast<const Collections::CreateEventDcpData*>(
                            eventData.data());
            last_collection_id = ev->cid.to_host();
            last_scope_id = ev->sid.to_host();
            last_collection_manifest_uid = ev->manifestUid.to_host();
        } else {
            const auto expectedSize =
                    Collections::CreateWithMaxTtlEventDcpData::size;
            EXPECT_EQ(expectedSize, eventData.size());
            const auto* ev = reinterpret_cast<
                    const Collections::CreateWithMaxTtlEventDcpData*>(
                    eventData.data());
            last_collection_id = ev->cid.to_host();
            last_scope_id = ev->sid.to_host();
            last_collection_manifest_uid = ev->manifestUid.to_host();
        }

        last_key.assign(reinterpret_cast<const char*>(key.data()), key.size());
        break;
    }
    case mcbp::systemevent::id::DeleteCollection: {
        const auto* ev = reinterpret_cast<const Collections::DropEventDcpData*>(
                eventData.data());
        last_collection_id = ev->cid.to_host();
        last_collection_manifest_uid = ev->manifestUid.to_host();
        // Using the ::size directly in the EXPECT is failing to link on
        // OSX build, but copying the value works.
        const auto expectedSize = Collections::DropEventDcpData::size;
        EXPECT_EQ(expectedSize, eventData.size());
        EXPECT_EQ(nullptr, key.data());
        break;
    }
    case mcbp::systemevent::id::CreateScope: {
        const auto* ev =
                reinterpret_cast<const Collections::CreateScopeEventDcpData*>(
                        eventData.data());
        last_scope_id = ev->sid.to_host();
        last_collection_manifest_uid = ev->manifestUid.to_host();

        const auto expectedSize = Collections::CreateScopeEventDcpData::size;
        EXPECT_EQ(expectedSize, eventData.size());
        last_key.assign(reinterpret_cast<const char*>(key.data()), key.size());
        break;
    }
    case mcbp::systemevent::id::DropScope: {
        const auto* ev =
                reinterpret_cast<const Collections::DropScopeEventDcpData*>(
                        eventData.data());
        last_scope_id = ev->sid.to_host();
        last_collection_manifest_uid = ev->manifestUid.to_host();

        const auto expectedSize = Collections::DropScopeEventDcpData::size;
        EXPECT_EQ(expectedSize, eventData.size());
        EXPECT_EQ(nullptr, key.data());
        break;
    }
    default:
        EXPECT_TRUE(false) << "Unsupported event " << int(event);
    }

    if (consumer) {
        auto rv = consumer->systemEvent(
                opaque, replicaVB, event, bySeqno, version, key, eventData);
        EXPECT_EQ(ENGINE_SUCCESS, rv)
                << "Failure to push system-event onto the consumer";
        return rv;
    }
    return ENGINE_SUCCESS;
}

ENGINE_ERROR_CODE CollectionsDcpTestProducers::marker(
        uint32_t opaque,
        Vbid vbucket,
        uint64_t start_seqno,
        uint64_t end_seqno,
        uint32_t flags,
        std::optional<uint64_t> high_completed_seqno,
        std::optional<uint64_t> maxVisibleSeqno,
        std::optional<uint64_t> timestamp,
        cb::mcbp::DcpStreamId sid) {
    auto ret = ENGINE_SUCCESS;
    if (consumer) {
        ret = consumer->snapshotMarker(opaque,
                                       replicaVB,
                                       start_seqno,
                                       end_seqno,
                                       flags,
                                       high_completed_seqno,
                                       maxVisibleSeqno);
        EXPECT_EQ(ENGINE_SUCCESS, ret);
    }

    MockDcpMessageProducers::marker(opaque,
                                    replicaVB,
                                    start_seqno,
                                    end_seqno,
                                    flags,
                                    high_completed_seqno,
                                    maxVisibleSeqno,
                                    timestamp,
                                    sid);

    return ret;
}

ENGINE_ERROR_CODE CollectionsDcpTestProducers::mutation(
        uint32_t opaque,
        cb::unique_item_ptr itm,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t lock_time,
        uint8_t nru,
        cb::mcbp::DcpStreamId sid) {
    auto ret = ENGINE_SUCCESS;
    if (consumer) {
        auto& item = *static_cast<Item*>(itm.get());

        ret = consumer->mutation(
                opaque,
                item.getKey(),
                {reinterpret_cast<const uint8_t*>(item.getData()),
                 item.getNBytes()},
                0,
                item.getDataType(),
                item.getCas(),
                replicaVB,
                item.getFlags(),
                by_seqno,
                rev_seqno,
                item.getExptime(),
                lock_time,
                {},
                nru);
    }

    MockDcpMessageProducers::mutation(opaque,
                                      std::move(itm),
                                      vbucket,
                                      by_seqno,
                                      rev_seqno,
                                      lock_time,
                                      nru,
                                      sid);
    return ret;
}

ENGINE_ERROR_CODE CollectionsDcpTestProducers::prepare(
        uint32_t opaque,
        cb::unique_item_ptr itm,
        Vbid vbucket,
        uint64_t by_seqno,
        uint64_t rev_seqno,
        uint32_t lock_time,
        uint8_t nru,
        DocumentState document_state,
        cb::durability::Level level) {
    auto ret = ENGINE_SUCCESS;
    if (consumer) {
        auto& item = *static_cast<Item*>(itm.get());

        ret = consumer->prepare(
                opaque,
                item.getKey(),
                {reinterpret_cast<const uint8_t*>(item.getData()),
                 item.getNBytes()},
                0,
                item.getDataType(),
                item.getCas(),
                replicaVB,
                item.getFlags(),
                by_seqno,
                rev_seqno,
                item.getExptime(),
                lock_time,
                nru,
                document_state,
                level);
    }
    MockDcpMessageProducers::prepare(opaque,
                                     std::move(itm),
                                     vbucket,
                                     by_seqno,
                                     rev_seqno,
                                     lock_time,
                                     nru,
                                     document_state,
                                     level);
    return ret;
}

ENGINE_ERROR_CODE CollectionsDcpTestProducers::commit(uint32_t opaque,
                                                      Vbid vbucket,
                                                      const DocKey& key,
                                                      uint64_t prepare_seqno,
                                                      uint64_t commit_seqno) {
    auto ret = ENGINE_SUCCESS;
    if (consumer) {
        ret = consumer->commit(
                opaque, replicaVB, key, prepare_seqno, commit_seqno);
    }
    MockDcpMessageProducers::commit(
            opaque, vbucket, key, prepare_seqno, commit_seqno);
    return ret;
}