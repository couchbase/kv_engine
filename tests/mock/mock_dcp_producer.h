#pragma once

#include "dcp/producer.h"

/*
 * Mock of the DcpProducer class.  Wraps the real DcpProducer, but exposes
 * normally protected methods publically for test purposes.
 */
class MockDcpProducer: public DcpProducer {
public:
    MockDcpProducer(EventuallyPersistentEngine &theEngine, const void *cookie,
                    const std::string &name, bool isNotifier)
    : DcpProducer(theEngine, cookie, name, isNotifier)
    {}

    ENGINE_ERROR_CODE maybeSendNoop(struct dcp_message_producers* producers)
    {
        return DcpProducer::maybeSendNoop(producers);
    }

    void setNoopSendTime(const rel_time_t timeValue) {
        noopCtx.sendTime = timeValue;
    }

    rel_time_t getNoopSendTime() {
        return noopCtx.sendTime;
    }

    bool getNoopPendingRecv() {
        return noopCtx.pendingRecv;
    }

    void setNoopEnabled(const bool booleanValue) {
        noopCtx.enabled = booleanValue;
    }

    bool getNoopEnabled() {
        return noopCtx.enabled;
    }
};