/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

// Expose the compile-time selected allocator hooks implementation:

#include "alloc_hooks_dummy.h"
#include "alloc_hooks_jemalloc.h"

#if defined(HAVE_JEMALLOC)
using AllocHooks = JemallocHooks;
#else
using AllocHooks = DummyAllocHooks;
#endif

