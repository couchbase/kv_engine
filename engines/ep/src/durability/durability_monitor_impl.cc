/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2019 Couchbase, Inc.
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

#include "durability_monitor_impl.h"
#include <folly/lang/Assume.h>
#include <gsl.h>

/// Helper function to determine the expiry time for a SyncWrite from the
/// durability requirements.
static boost::optional<std::chrono::steady_clock::time_point>
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

DurabilityMonitor::SyncWrite::SyncWrite(
        const void* cookie,
        queued_item item,
        std::chrono::milliseconds defaultTimeout,
        const ReplicationChain* firstChain,
        const ReplicationChain* secondChain)
    : cookie(cookie),
      item(item),
      expiryTime(expiryFromDurabiltyReqs(item->getDurabilityReqs(),
                                         defaultTimeout)) {
    // We should always have a first chain if we have a second
    if (secondChain) {
        Expects(firstChain);
    }

    if (firstChain) {
        resetTopology(*firstChain, secondChain);
    }
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

const void* DurabilityMonitor::SyncWrite::getCookie() const {
    return cookie;
}

void DurabilityMonitor::SyncWrite::ack(const std::string& node) {
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

bool DurabilityMonitor::SyncWrite::isSatisfied() const {
    if (!firstChain) {
        throw std::logic_error(
                "SyncWrite::isSatisfied: Attmpting to check "
                "durability requirements are met without the "
                "first replication chain");
    }

    bool ret{false};

    auto firstChainSatisfied =
            firstChain.ackCount >= firstChain.chainPtr->majority;
    auto firstChainActiveSatisfied = firstChain.chainPtr->hasAcked(
            firstChain.chainPtr->active, this->getBySeqno());
    auto secondChainSatisfied =
            !secondChain ||
            secondChain.ackCount >= secondChain.chainPtr->majority;
    auto secondChainActiveSatisfied =
            !secondChain ||
            (secondChain.chainPtr->active == firstChain.chainPtr->active ||
             secondChain.chainPtr->hasAcked(secondChain.chainPtr->active,
                                            this->getBySeqno()));

    switch (getDurabilityReqs().getLevel()) {
    case cb::durability::Level::Majority:
        ret = firstChainSatisfied && secondChainSatisfied &&
              secondChainActiveSatisfied;
        break;
    case cb::durability::Level::PersistToMajority:
    case cb::durability::Level::MajorityAndPersistOnMaster:
        ret = firstChainSatisfied && firstChainActiveSatisfied &&
              secondChainSatisfied && secondChainActiveSatisfied;
        break;
    case cb::durability::Level::None:
        throw std::logic_error("SyncWrite::isSatisfied: Level::None");
    }

    return ret;
}

bool DurabilityMonitor::SyncWrite::isExpired(
        std::chrono::steady_clock::time_point asOf) const {
    if (!expiryTime) {
        return false;
    }
    return expiryTime < asOf;
}

void DurabilityMonitor::SyncWrite::resetTopology(
        const ReplicationChain& firstChain,
        const ReplicationChain* secondChain) {
    this->firstChain.reset(&firstChain);
    this->secondChain.reset(secondChain);

    // We can call resetTopology in one of two cases:
    // a) for a new SyncWrite
    // b) for a topology change
    // In both cases, creating a SyncWrite is only valid if durability is
    // possible for each chain in the given topology.
    // This condition should be checked and dealt with by the caller but
    // we will enforce it here to defend from any races in the setting of a new
    // topology.
    Expects(firstChain.isDurabilityPossible());

    if (secondChain) {
        Expects(secondChain->isDurabilityPossible());
    }
}

std::ostream& operator<<(std::ostream& os,
                         const DurabilityMonitor::SyncWrite& sw) {
    os << "SW @" << &sw << " "
       << "cookie:" << sw.cookie << " qi:[key:'" << sw.item->getKey()
       << "' seqno:" << sw.item->getBySeqno()
       << " reqs:" << to_string(sw.item->getDurabilityReqs()) << "] maj:"
       << std::to_string(sw.firstChain ? sw.firstChain.chainPtr->majority : 0)
       << " #1stChainAcks:" << std::to_string(sw.firstChain.ackCount)
       << " 1stChainAcks:[";
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
    os << acks << "] #2ndChainAcks:" << std::to_string(sw.secondChain.ackCount)
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

DurabilityMonitor::ReplicationChain::ReplicationChain(
        const DurabilityMonitor::ReplicationChainName name,
        const std::vector<std::string>& nodes,
        const Container::iterator& it)
    : majority(nodes.size() / 2 + 1), active(nodes.at(0)), name(name) {
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
        if (!positions.emplace(node, Position(it)).second) {
            throw std::invalid_argument(
                    "ReplicationChain::ReplicationChain: Duplicate node: " +
                    node);
        }
    }
}

size_t DurabilityMonitor::ReplicationChain::size() const {
    return positions.size();
}

bool DurabilityMonitor::ReplicationChain::isDurabilityPossible() const {
    Expects(size());
    Expects(majority);
    return size() >= majority;
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

bool DurabilityMonitor::ReplicationChain::hasAcked(const std::string& node,
                                                   int64_t bySeqno) const {
    return positions.at(node).lastWriteSeqno >= bySeqno;
}
