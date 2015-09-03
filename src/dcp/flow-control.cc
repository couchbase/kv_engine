/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc
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

#include "ep_engine.h"
#include "config.h"
#include "objectregistry.h"

#include "dcp/flow-control.h"

FlowControl::FlowControl(EventuallyPersistentEngine &engine,
                         DcpConsumer* consumer) :
    consumerConn(consumer),
    engine_(engine),
    pendingControl(true),
    lastBufferAck(ep_current_time()),
    ackedBytes(0),
    freedBytes(0)
{
    enabled = engine.getDcpFlowControlManager().isEnabled();
    if (enabled) {
        bufferSize =
                    engine.getDcpFlowControlManager().newConsumerConn(consumer);
    }
}

FlowControl::~FlowControl()
{
    engine_.getDcpFlowControlManager().handleDisconnect(consumerConn);
}

ENGINE_ERROR_CODE FlowControl::handleFlowCtl(
                                    struct dcp_message_producers* producers)
{
    if (enabled) {
        ENGINE_ERROR_CODE ret;
        uint32_t ackable_bytes = freedBytes.load();
        SpinLockHolder lh(&bufferSizeLock);
        if (pendingControl) {
            pendingControl = false;
            std::string buf_size(std::to_string(bufferSize));
            lh.unlock();
            uint64_t opaque = consumerConn->incrOpaqueCounter();
            const std::string &controlMsgKey = consumerConn->getControlMsgKey();
            EventuallyPersistentEngine *epe =
                                    ObjectRegistry::onSwitchThread(NULL, true);
            ret = producers->control(consumerConn->getCookie(), opaque,
                                     controlMsgKey.c_str(),
                                     controlMsgKey.length(),
                                     buf_size.c_str(),
                                     buf_size.length());
            ObjectRegistry::onSwitchThread(epe);
            return (ret == ENGINE_SUCCESS) ? ENGINE_WANT_MORE : ret;
        } else if (isBufferSufficientlyDrained_UNLOCKED(ackable_bytes)) {
            lh.unlock();
            /* Send a buffer ack when at least 20% of the buffer is drained */
            uint64_t opaque = consumerConn->incrOpaqueCounter();
            EventuallyPersistentEngine *epe =
                                    ObjectRegistry::onSwitchThread(NULL, true);
            ret = producers->buffer_acknowledgement(consumerConn->getCookie(),
                                                    opaque, 0, ackable_bytes);
            ObjectRegistry::onSwitchThread(epe);
            lastBufferAck = ep_current_time();
            ackedBytes.fetch_add(ackable_bytes);
            freedBytes.fetch_sub(ackable_bytes);
            return (ret == ENGINE_SUCCESS) ? ENGINE_WANT_MORE : ret;
        } else if (ackable_bytes > 0 &&
                   (ep_current_time() - lastBufferAck) > 5) {
            lh.unlock();
            /* Ack at least every 5 seconds */
            uint64_t opaque = consumerConn->incrOpaqueCounter();
            EventuallyPersistentEngine *epe =
                                    ObjectRegistry::onSwitchThread(NULL, true);
            ret = producers->buffer_acknowledgement(consumerConn->getCookie(),
                                                    opaque, 0, ackable_bytes);
            ObjectRegistry::onSwitchThread(epe);
            lastBufferAck = ep_current_time();
            ackedBytes.fetch_add(ackable_bytes);
            freedBytes.fetch_sub(ackable_bytes);
            return (ret == ENGINE_SUCCESS) ? ENGINE_WANT_MORE : ret;
        } else {
            lh.unlock();
        }
    }
    return ENGINE_FAILED;
}

void FlowControl::incrFreedBytes(uint32_t bytes)
{
    freedBytes.fetch_add(bytes);
}

uint32_t FlowControl::getFlowControlBufSize(void)
{
    SpinLockHolder lh(&bufferSizeLock);
    return bufferSize;
}

void FlowControl::setFlowControlBufSize(uint32_t newSize)
{
    SpinLockHolder lh(&bufferSizeLock);
    if (newSize != bufferSize) {
        bufferSize = newSize;
        pendingControl = true;
    }
}

bool FlowControl::isBufferSufficientlyDrained() {
    SpinLockHolder lh(&bufferSizeLock);
    return isBufferSufficientlyDrained_UNLOCKED(freedBytes.load());
}

bool FlowControl::isBufferSufficientlyDrained_UNLOCKED(uint32_t ackable_bytes) {
    return ackable_bytes > (bufferSize * .2);
}

void FlowControl::addStats(ADD_STAT add_stat, const void *c)
{
    consumerConn->addStat("total_acked_bytes", ackedBytes, add_stat, c);
    consumerConn->addStat("max_buffer_bytes", bufferSize, add_stat, c);
}
