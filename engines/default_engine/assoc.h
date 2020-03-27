#pragma once

#include <memcached/engine_error.h>

#include "items.h"

/* associative array */
ENGINE_ERROR_CODE assoc_init(struct default_engine *engine);
void assoc_destroy();
hash_item *assoc_find(uint32_t hash, const hash_key* key);
int assoc_insert(uint32_t hash, hash_item *item);
void assoc_delete(uint32_t hash, const hash_key* key);
bool assoc_expanding();
