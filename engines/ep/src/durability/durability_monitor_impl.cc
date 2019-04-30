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

DurabilityMonitor::SyncWrite::SyncWrite(const void* cookie,
                                        queued_item item,
                                        const ReplicationChain* firstChain)
    : cookie(cookie),
      item(item),
      expiryTime(
              item->getDurabilityReqs().getTimeout()
                      ? std::chrono::steady_clock::now() +
                                std::chrono::milliseconds(
                                        item->getDurabilityReqs().getTimeout())
                      : boost::optional<
                                std::chrono::steady_clock::time_point>{}) {
    if (firstChain) {
        resetTopology(*firstChain);
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

    if (firstChain.chainPtr->positions.find(node) ==
        firstChain.chainPtr->positions.end()) {
        throw std::logic_error("SyncWrite::ack: Node not valid: " + node);
    }

    firstChain.ackCount++;
}

bool DurabilityMonitor::SyncWrite::isSatisfied() const {
    if (!firstChain) {
        throw std::logic_error(
                "SyncWrite::isSatisfied: Attmpting to check "
                "durability requirements are met without the "
                "first replication chain");
    }

    bool ret{false};

    switch (getDurabilityReqs().getLevel()) {
    case cb::durability::Level::Majority:
        ret = firstChain.ackCount >= firstChain.chainPtr->majority;
        break;
    case cb::durability::Level::PersistToMajority:
    case cb::durability::Level::MajorityAndPersistOnMaster:
        ret = firstChain.ackCount >= firstChain.chainPtr->majority &&
              firstChain.chainPtr->hasAcked(firstChain.chainPtr->active,
                                            this->getBySeqno());
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
        const ReplicationChain& firstChain) {
    this->firstChain.reset(&firstChain);

    // We are making a SyncWrite for tracking, we must have already ensured
    // that the Durability Requirements can be met at this point.
    Expects(firstChain.size() >= firstChain.majority);
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
