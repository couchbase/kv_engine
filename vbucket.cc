/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

#include "vbucket.hh"

VBucketFirer::VBucketFirer(const SERVER_CORE_API *c, ENGINE_ERROR_CODE r)
    : core(c), code(r) {
}

void VBucketFirer::operator()(const void *cookie) {
    core->notify_io_complete(cookie, code);
}

void VBucket::fireAllOps(SERVER_CORE_API *core, ENGINE_ERROR_CODE code) {
    LockHolder lh(pendingOpLock);
    VBucketFirer vbf(core, code);
    std::for_each(pendingOps.begin(), pendingOps.end(), vbf);
    pendingOps.clear();
}
