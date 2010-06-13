/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include <functional>

#include "vbucket.hh"

void VBucket::fireAllOps(SERVER_CORE_API *core, ENGINE_ERROR_CODE code) {
    LockHolder lh(pendingOpLock);
    std::for_each(pendingOps.begin(), pendingOps.end(),
                  std::bind2nd(std::ptr_fun(core->notify_io_complete), code));
    pendingOps.clear();
}
