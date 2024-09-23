/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "durability_monitor_impl.h"
#include "durability/durability_monitor.h"
#include <fmt/core.h>
#include <folly/lang/Assume.h>
#include <gsl/gsl-lite.hpp>
#include <utilities/logtags.h>
#include <utility>

class CookieIface;

/// Helper function to determine the expiry time for a SyncWrite from the
/// durability requirements.
static std::optional<std::chrono::steady_clock::time_point>
expiryFromDurabiltyReqs(const cb::durability::Requirements& reqs,
                        std::chrono::milliseconds defaultTimeout) {
    auto timeout = reqs.getTimeout();
    if (timeout.isInfinite()) {
        return {};
    }

    auto relativeTimeout = timeout.isDefault()
                                   ? defaultTimeout
                                   : std::chrono::milliseconds(timeout.get());
    return std::chrono::steady_clock::now() + relativeTimeout;
}

DurabilityMonitor::SyncWrite::SyncWrite(queued_item item)
    : item(std::move(item)), startTime(std::chrono::steady_clock::now()) {
}

const StoredDocKey& DurabilityMonitor::SyncWrite::getKey() const {
    return item->getKey();
}

int64_t DurabilityMonitor::SyncWrite::getBySeqno() const {
    return item->getBySeqno();
}

cb::durability::Requirements DurabilityMonitor::SyncWrite::getDurabilityReqs()
        const {
    return item->getDurabilityReqs();
}

DurabilityMonitor::ActiveSyncWrite::ActiveSyncWrite(
        CookieIface* cookie,
        queued_item item,
        std::chrono::milliseconds defaultTimeout,
        const ActiveDurabilityMonitor::ReplicationChain* firstChain,
        const ActiveDurabilityMonitor::ReplicationChain* secondChain)
    : SyncWrite(std::move(item)),
      cookie(cookie),
      expiryTime(expiryFromDurabiltyReqs(getDurabilityReqs(), defaultTimeout)) {
    initialiseChains(firstChain, secondChain);
}

DurabilityMonitor::ActiveSyncWrite::ActiveSyncWrite(
        CookieIface* cookie,
        queued_item item,
        const ActiveDurabilityMonitor::ReplicationChain* firstChain,
        const ActiveDurabilityMonitor::ReplicationChain* secondChain,
        InfiniteTimeout)
    : SyncWrite(std::move(item)), cookie(cookie) {
    Expects(getDurabilityReqs().getLevel() != cb::durability::Level::None);
    // If creating a SyncWrite with Infinite timeout then we could be
    // re-creating SyncWrites from warmup - in which case durability may not
    // be possible with the current topology - for example:
    // 1) Prior to warmup, nodes=2, replicas=1 - topology = [active, replica].
    // 2) SyncWrite was initially added normally (with valid topology) to
    //    ActiveDM.
    // 3) Replica node is removed, toplogy is updated to [active, <null>].
    //    SyncWrite still pending.
    // 4) Restart active and warmup. At this point the persisted toplogy from
    //    (3) doesn't have durability possible.
    //
    // As such, call resetTopology() directly here and not initialiseChains
    // which performs durability possible checks.
    if (firstChain) {
        resetTopology(*firstChain, secondChain);
    }
}

DurabilityMonitor::ActiveSyncWrite::ActiveSyncWrite(SyncWrite&& write)
    : SyncWrite(write), cookie(nullptr) {
    initialiseChains(nullptr /*firstChain*/, nullptr /*secondChain*/);
}

void DurabilityMonitor::ActiveSyncWrite::initialiseChains(
        const ActiveDurabilityMonitor::ReplicationChain* firstChain,
        const ActiveDurabilityMonitor::ReplicationChain* secondChain) {
    // We should always have a first chain if we have a second
    if (secondChain) {
        Expects(firstChain);
    }

    if (firstChain) {
        checkDurabilityPossibleAndResetTopology(*firstChain, secondChain);
    }
}

void DurabilityMonitor::ActiveSyncWrite::resetChains() {
    this->firstChain.reset(nullptr, 0 /*ackCount*/);
    this->secondChain.reset(nullptr, 0 /*ackCount*/);
}

CookieIface* DurabilityMonitor::ActiveSyncWrite::getCookie() const {
    return cookie;
}

void DurabilityMonitor::ActiveSyncWrite::clearCookie() {
    cookie = nullptr;
}

std::chrono::steady_clock::time_point
DurabilityMonitor::ActiveSyncWrite::getStartTime() const {
    return startTime;
}

std::optional<std::chrono::steady_clock::time_point>
DurabilityMonitor::ActiveSyncWrite::getExpiryTime() const {
    return expiryTime;
}

void DurabilityMonitor::ActiveSyncWrite::ack(const std::string& node) {
    if (!firstChain) {
        throw std::logic_error(
                "SyncWrite::ack: Acking without a ReplicationChain");
    }

    // The node to ack may be in the firstChain, secondChain, or both, but we
    // don't know which.
    bool acked = false;
    if (firstChain.chainPtr->positions.find(node) !=
        firstChain.chainPtr->positions.end()) {
        firstChain.ackCount++;
        acked = true;
    }

    if (secondChain && secondChain.chainPtr->positions.find(node) !=
                               secondChain.chainPtr->positions.end()) {
        secondChain.ackCount++;
        acked = true;
    }

    if (!acked) {
        throw std::logic_error("SyncWrite::ack: Node not valid: " + node);
    }
}

bool DurabilityMonitor::ActiveSyncWrite::isSatisfied() const {
    if (!firstChain) {
        throw std::logic_error(
                "SyncWrite::isSatisfied: Attmpting to check "
                "durability requirements are met without the "
                "first replication chain");
    }

    bool ret{false};

    auto firstChainSatisfied = firstChain.isSatisfied();
    auto firstChainActiveSatisfied = firstChain.chainPtr->hasAcked(
            firstChain.chainPtr->active, this->getBySeqno());
    auto secondChainSatisfied = !secondChain || secondChain.isSatisfied();
    auto secondChainActiveSatisfied =
            !secondChain ||
            (secondChain.chainPtr->active == firstChain.chainPtr->active ||
             secondChain.chainPtr->hasAcked(secondChain.chainPtr->active,
                                            this->getBySeqno()));

    // MB-35190: A SyncWrite must always be satisfied on the active, even
    // if it is a majority level prepare.
    // Though majority prepares are logically satisfied immediately on the
    // active, we need to make sure they have been acked by the active as
    // this means the HPS has reached the prepare (i.e., is not waiting
    // at an earlier durability fence).
    ret = firstChainSatisfied && firstChainActiveSatisfied &&
          secondChainSatisfied && secondChainActiveSatisfied;

    return ret;
}

SyncWriteStatus DurabilityMonitor::ActiveSyncWrite::getStatusForCommit() const {
    if (!firstChain) {
        throw std::logic_error(
                "SyncWrite::isSatisfied: Attmpting to check replica acks "
                "without the first replication chain");
    }

    const bool isDurable =
            firstChain.ackCount >= firstChain.chainPtr->majority &&
            (!secondChain ||
             secondChain.ackCount > firstChain.chainPtr->majority);
    return isDurable ? SyncWriteStatus::ToCommit
                     : SyncWriteStatus::ToCommitNotDurable;
}

bool DurabilityMonitor::ActiveSyncWrite::isExpired(
        std::chrono::steady_clock::time_point asOf) const {
    if (!expiryTime) {
        return false;
    }
    return expiryTime < asOf;
}

uint8_t DurabilityMonitor::ActiveSyncWrite::getAckCountForNewChain(
        const ActiveDurabilityMonitor::ReplicationChain& chain) {
    auto ackCount = 0;
    for (const auto& pos : chain.positions) {
        // We only bump the ackCount if this SyncWrite was acked in either the
        // first or second chain in the old topology (if they exist).
        if (((this->firstChain &&
              this->firstChain.chainPtr->hasAcked(pos.first, getBySeqno())) ||
             (this->secondChain &&
              this->secondChain.chainPtr->hasAcked(pos.first, getBySeqno())))) {
            ackCount++;
        }
    }

    return ackCount;
}

void DurabilityMonitor::ActiveSyncWrite::resetTopology(
        const ActiveDurabilityMonitor::ReplicationChain& firstChain,
        const ActiveDurabilityMonitor::ReplicationChain* secondChain) {
    // We need to calculate our new ack counts for each chain before resetting
    // any topology so store these values first.
    auto ackedFirstChain = getAckCountForNewChain(firstChain);
    auto ackedSecondChain = 0;
    if (secondChain) {
        ackedSecondChain = getAckCountForNewChain(*secondChain);
    }

    this->firstChain.reset(&firstChain, ackedFirstChain);
    this->secondChain.reset(secondChain, ackedSecondChain);
}

void DurabilityMonitor::ActiveSyncWrite::
        checkDurabilityPossibleAndResetTopology(
                const ActiveDurabilityMonitor::ReplicationChain& firstChain,
                const ActiveDurabilityMonitor::ReplicationChain* secondChain) {
    // We can reset the topology in one of two cases:
    // a) for a new SyncWrite
    // b) for a topology change
    // In both cases, creating a SyncWrite is only valid if durability is
    // possible for each chain in the given topology.
    // This condition should be checked and dealt with by the caller but
    // we will enforce it here to defend from any races in the setting of a
    // new topology. Check these conditions before setting the topology of
    // the SyncWrite to ensure we don't end up with any bad
    // pointers/references.
    Expects(firstChain.isDurabilityPossible());

    if (secondChain) {
        Expects(secondChain->isDurabilityPossible());
    }

    resetTopology(firstChain, secondChain);
}
std::ostream& operator<<(std::ostream& os,
                         const DurabilityMonitor::SyncWrite& sw) {
    os << "SW @" << &sw << " "
       << "qi:[key:'" << cb::tagUserData(sw.item->getKey().to_string())
       << "' seqno:" << sw.item->getBySeqno()
       << " reqs:" << to_string(sw.item->getDurabilityReqs()) << "] "
       << " status:" << to_string(sw.getStatus());
    return os;
}

bool DurabilityMonitor::ActiveSyncWrite::ChainStatus::isSatisfied() const {
    Expects(chainPtr);

    // The SyncWrite has been acked by the majority of the nodes in the
    // replication topology.
    const bool hasMajority = ackCount >= chainPtr->majority;

    // The SyncWrite can be committed despite the lack of replicas for a
    // majority.
    const bool allowFallback =
            chainPtr->size() < chainPtr->majority &&
            chainPtr->commitStrategy ==
                    CommitStrategy::MajorityAckFallbackToMasterAckOnly;

    return hasMajority || allowFallback;
}

std::ostream& operator<<(std::ostream& os,
                         const DurabilityMonitor::ActiveSyncWrite& sw) {
    os << static_cast<const DurabilityMonitor::SyncWrite&>(sw) << " maj:"
       << std::to_string(sw.firstChain ? sw.firstChain.chainPtr->majority : 0)
       << " #1stChainAcks:" << sw.firstChain.ackCount << " 1stChainAcks:[";
    std::string acks;
    if (sw.firstChain) {
        for (const auto& ack : sw.firstChain.chainPtr->positions) {
            if (sw.firstChain.chainPtr->hasAcked(ack.first, sw.getBySeqno())) {
                if (!acks.empty()) {
                    acks += ",";
                }
                acks += ack.first;
            }
        }
    }
    os << acks << "] #2ndChainAcks:" << sw.secondChain.ackCount
       << " 2ndChainAcks:[";
    acks = "";
    if (sw.secondChain) {
        for (const auto& ack : sw.secondChain.chainPtr->positions) {
            if (sw.secondChain.chainPtr->hasAcked(ack.first, sw.getBySeqno())) {
                if (!acks.empty()) {
                    acks += ",";
                }
                acks += ack.first;
            }
        }
    }
    os << acks << "]";
    return os;
}

ActiveDurabilityMonitor::ReplicationChain::ReplicationChain(
        const DurabilityMonitor::ReplicationChainName name,
        const std::vector<std::string>& nodes,
        const Container::iterator& it,
        size_t maxAllowedReplicas,
        CommitStrategy commitStrategy,
        Vbid vbid)
    : majority(nodes.size() / 2 + 1),
      active(nodes.at(0)),
      maxAllowedReplicas(maxAllowedReplicas),
      commitStrategy(commitStrategy),
      name(name) {
    if (nodes.at(0) == UndefinedNode) {
        throw std::invalid_argument(
                "ReplicationChain::ReplicationChain: Active node cannot be "
                "undefined");
    }

    for (const auto& node : nodes) {
        if (node == UndefinedNode) {
            // unassigned, don't register a position in the chain.
            continue;
        }
        // This check ensures that there is no duplicate in the given chain
        auto [itr, success] = positions.emplace(node, Position<Container>(it));
        if (!success) {
            throw std::invalid_argument(
                    "ReplicationChain::ReplicationChain: Duplicate node: " +
                    node);
        }
        itr->second.lastAckSeqno.setLabeler({admRepChainLabel, vbid});
        itr->second.lastWriteSeqno.setLabeler({admRepChainLabel, vbid});
    }
}

size_t ActiveDurabilityMonitor::ReplicationChain::size() const {
    return positions.size();
}

bool ActiveDurabilityMonitor::ReplicationChain::isDurabilityPossible() const {
    Expects(size());
    Expects(majority);

    // MB-34453 / MB-34150: Disallow SyncWrites if the number of configured
    // replicas exceeds maxAllowedReplicas.
    if (size() > maxAllowedReplicas + 1) {
        return false;
    }

    switch (commitStrategy) {
    case CommitStrategy::MajorityAck:
        return size() >= majority;
    case CommitStrategy::MajorityAckFallbackToMasterAckOnly:
        return true;
    }
    folly::assume_unreachable();
}

std::string to_string(DurabilityMonitor::ReplicationChainName name) {
    switch (name) {
    case DurabilityMonitor::ReplicationChainName::First:
        return "First";
    case DurabilityMonitor::ReplicationChainName::Second:
        return "Second";
    }
    folly::assume_unreachable();
}

std::string_view format_as(DurabilityMonitor::CommitStrategy strategy) {
    switch (strategy) {
    case DurabilityMonitor::CommitStrategy::MajorityAck:
        return "MajorityAck";
    case DurabilityMonitor::CommitStrategy::MajorityAckFallbackToMasterAckOnly:
        return "MajorityAckFallbackToMasterAckOnly";
    }
    folly::assume_unreachable();
}

bool ActiveDurabilityMonitor::ReplicationChain::hasAcked(
        const std::string& node, int64_t bySeqno) const {
    auto itr = positions.find(node);

    // Replica has not acked for this chain if we do not know about it.
    if (itr == positions.end()) {
        return false;
    }

    return itr->second.lastWriteSeqno >= bySeqno;
}

bool operator>(const SnapshotEndInfo& a, const SnapshotEndInfo& b) {
    return a.seqno > b.seqno;
}
std::string to_string(const SnapshotEndInfo& snapshotEndInfo) {
    return std::to_string(snapshotEndInfo.seqno) + "{" +
           to_string(snapshotEndInfo.type) + "}";
}

std::string to_string(SyncWriteStatus status) {
    switch (status) {
    case SyncWriteStatus::Pending:
        return "Pending";
    case SyncWriteStatus::ToCommit:
        return "ToCommit";
    case SyncWriteStatus::ToCommitNotDurable:
        return "ToCommitNotDurable";
    case SyncWriteStatus::ToAbort:
        return "ToAbort";
    case SyncWriteStatus::Completed:
        return "Completed";
    }
    folly::assume_unreachable();
}

std::string ActiveDurabilityMonitor::State::Labeller::getLabel(
        const char* name) const {
    return fmt::format("ActiveDM({})::State::{}", vbid.to_string(), name);
}

ActiveDurabilityMonitor::State::SeqnoAckQueueLabeller::SeqnoAckQueueLabeller(
        std::string nodeName, Vbid vbid)
    :
#if CB_DEVELOPMENT_ASSERTS
      node(std::move(nodeName)),
#endif
      vbid(vbid) {
}

std::string ActiveDurabilityMonitor::State::SeqnoAckQueueLabeller::getLabel(
        const char* l) const {
    std::string nodeInfo;
#if CB_DEVELOPMENT_ASSERTS
    nodeInfo = " node:" + node;
#endif
    return fmt::format("ActiveDM({})::State::queuedSeqnoAck{}",
                       vbid.to_string(),
                       nodeInfo);
}

std::string PositionLabeller::getLabel(const char* name) const {
    std::string prefixStr = prefix ? prefix : "";
    return fmt::format("{}.{} ({})", prefixStr, name, vbid.to_string());
}
