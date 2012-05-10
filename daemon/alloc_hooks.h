
#ifndef MEM_HOOKS_H
#define MEM_HOOKS_H

#include "config.h"
#include <memcached/allocator_hooks.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#ifndef __cplusplus
#include <stdbool.h>
#endif

#include "memcached/extension_loggers.h"

typedef union func_ptr {
    void* (*func)();
    void* ptr;
} func_ptr;

typedef enum alloc_hooks_type {
    unknown = 0,
    tcmalloc
} alloc_hooks_type;

void init_alloc_hooks(void);

bool mc_add_new_hook(void (*)(const void* ptr, size_t size));
bool mc_remove_new_hook(void (*)(const void* ptr, size_t size));
bool mc_add_delete_hook(void (*)(const void* ptr));
bool mc_remove_delete_hook(void (*)(const void* ptr));
void mc_get_allocator_stats(allocator_stats*);
int mc_get_extra_stats_size(void);
size_t mc_get_allocation_size(void*);
void mc_get_detailed_stats(char*, int);

#endif /* MEM_HOOKS_H */
