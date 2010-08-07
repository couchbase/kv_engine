/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include <functional>

#include "vbucket.hh"

const vbucket_state_t VBucket::ACTIVE = static_cast<vbucket_state_t>(htonl(active));
const vbucket_state_t VBucket::REPLICA = static_cast<vbucket_state_t>(htonl(replica));
const vbucket_state_t VBucket::PENDING = static_cast<vbucket_state_t>(htonl(pending));
const vbucket_state_t VBucket::DEAD = static_cast<vbucket_state_t>(htonl(dead));

void VBucket::fireAllOps(SERVER_CORE_API *core, ENGINE_ERROR_CODE code) {
    LockHolder lh(pendingOpLock);
    std::for_each(pendingOps.begin(), pendingOps.end(),
                  std::bind2nd(std::ptr_fun(core->notify_io_complete), code));
    pendingOps.clear();
}

void VBucket::setState(vbucket_state_t to, SERVER_CORE_API *core) {
    assert(core);
    vbucket_state_t oldstate(state);

    getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                     "Beginning vbucket transition of %d from %s to %s\n",
                     id, VBucket::toString(oldstate), VBucket::toString(to));

    state = to;
    if (to == active) {
        fireAllOps(core, ENGINE_SUCCESS);
    } else if (to == pending) {
        // Nothing
    } else {
        fireAllOps(core, ENGINE_NOT_MY_VBUCKET);
    }

    getLogger()->log(EXTENSION_LOG_INFO, NULL,
                     "Completed vbucket transition of %d from %s to %s\n",
                     id, VBucket::toString(oldstate), VBucket::toString(to));
}
