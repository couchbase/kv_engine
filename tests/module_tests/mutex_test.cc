#include "config.h"

#include <cassert>
#include <iostream>

#include "common.h"
#include "locks.h"

int main(int argc, char **argv) {
    (void)argc; (void)argv;

    Mutex m;
    assert(!m.ownsLock());
    {
        LockHolder lh(m);
        assert(m.ownsLock());
    }
    assert(!m.ownsLock());

    return 0;
}
