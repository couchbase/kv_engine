/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#include <platform/platform.h>
#include <cJSON.h>

#include <stdint.h>

#ifdef __cplusplus
#include <cstring>
#include <string>
extern "C" {
#endif

    typedef struct profile_st {
        cb_mutex_t mutex;
        bool deleted;
        int refcount;

        uint8_t cmd[0x100];
#ifdef __cplusplus
        std::string name;
        std::string descr;

        profile_st() : deleted(false), refcount(0) {
            cb_mutex_initialize(&mutex);
            std::memset(cmd, 0, sizeof(cmd));
        }

        ~profile_st() {
            cb_mutex_enter(&mutex);
            cb_assert(deleted);
            cb_assert(refcount == 0);
            cb_mutex_exit(&mutex);
            cb_mutex_destroy(&mutex);
        }

        void initialize(cJSON *root);
    private:
        void parseCommands(cJSON *obj, uint8_t mask);
#endif
    } profile_t;

    void initialize_profiles(void);

    int load_profiles_from_json(cJSON *root);

    int load_profiles_from_file(const char *fname);

    void destroy_profiles(void);

    profile_t *find_profile(const char *name);
    void release_profile(profile_t *profile);

#ifdef __cplusplus
    void loadProfilesFromJson(cJSON *root);
}
#endif
