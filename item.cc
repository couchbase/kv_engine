/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "item.hh"

static void devnull(uint64_t current)
{
    (void)current;
}

uint64_t Item::casCounter = 0;
uint64_t Item::casNotificationFrequency = 10000;
void (*Item::casNotifier)(uint64_t) = devnull;
Mutex Item::casMutex;
