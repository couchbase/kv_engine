#include "config.h"
#include <cassert>
#include <iostream>
#include <unistd.h>

#include "common.hh"
#include "locks.hh"

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
