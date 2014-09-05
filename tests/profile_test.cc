/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "daemon/profile.h"

#include <memcached/protocol_binary.h>
#include <iostream>
#include <list>
#include <stdlib.h>

static void test_null_json(void) {
    std::cout << "Testing NULL" << std::endl;
    try {
        loadProfilesFromJson(NULL);
        std::cerr << "Test fail: loadProfilesFromJson(NULL) should throw error"
                  << std::endl;
        exit(1);
    } catch (std::string &err) {

    }
}

static void test_missing_profiles(void) {
    std::cout << "Testing missing profile element" << std::endl;
    cJSON *root = cJSON_CreateObject();
    try {
        loadProfilesFromJson(root);
        std::cerr << "Test fail: loadProfilesFromJson({}) should throw error"
                  << std::endl;
        exit(1);
    } catch (std::string &err) {

    }
    cJSON_Delete(root);
}

static void test_invalid_profiles(void) {
    std::cout << "Testing invalid profile element" << std::endl;
    cJSON *root = cJSON_CreateObject();
    try {
        cJSON_AddTrueToObject(root, "profiles");
        loadProfilesFromJson(root);
        std::cerr << "Test fail: loadProfilesFromJson({profiles:true}) should throw error"
                  << std::endl;
        exit(1);

    } catch (std::string &err) {

    }
    cJSON_Delete(root);
}

static void test_empty_profiles(void) {
    std::cout << "Testing empty profile element" << std::endl;
    cJSON *root = cJSON_CreateObject();
    try {
        cJSON *array = cJSON_CreateArray();
        cJSON_AddItemToObject(root, "profiles", array);
        loadProfilesFromJson(root);
    } catch (std::string &err) {
        std::cerr << "Test fail: loadProfilesFromJson({profiles:[]}) should not throw error"
                  << std::endl;
        exit(1);

    }
    cJSON_Delete(root);
}

static void test_profile_missing_name(void) {
    std::cout << "Testing profile missing name" << std::endl;
    cJSON* profiles = cJSON_CreateObject();

    cJSON* profile = cJSON_CreateObject();
    cJSON_AddStringToObject(profile, "description", "descr");

    cJSON* obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "allow", "all");
    cJSON_AddStringToObject(obj, "disallow", "none");
    cJSON_AddItemToObject(profile, "memcached", obj);

    cJSON *array = cJSON_CreateArray();
    cJSON_AddItemToArray(array, profile);
    cJSON_AddItemToObject(profiles, "profiles", array);

    try {
        loadProfilesFromJson(profiles);
        std::cerr << "Test fail: loadProfilesFromJson(" <<
            cJSON_PrintUnformatted(profiles) << ") should throw error"
                  << std::endl;
        exit(1);
    } catch (std::string &err) {

    }

    cJSON_Delete(profiles);
}

static void test_profile_missing_description(void) {
    std::cout << "Testing profile missing description" << std::endl;
    cJSON* profiles = cJSON_CreateObject();

    cJSON* profile = cJSON_CreateObject();
    cJSON_AddStringToObject(profile, "name", "name");

    cJSON* obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "allow", "all");
    cJSON_AddStringToObject(obj, "disallow", "none");
    cJSON_AddItemToObject(profile, "memcached", obj);

    cJSON *array = cJSON_CreateArray();
    cJSON_AddItemToArray(array, profile);
    cJSON_AddItemToObject(profiles, "profiles", array);

    try {
        loadProfilesFromJson(profiles);
        std::cerr << "Test fail: loadProfilesFromJson(" <<
            cJSON_PrintUnformatted(profiles) << ") should throw error"
                  << std::endl;
        exit(1);
    } catch (std::string &err) {

    }

    cJSON_Delete(profiles);
}

static void test_profile_missing_memcached(void) {
    std::cout << "Testing profile missing memcached" << std::endl;
    cJSON* profiles = cJSON_CreateObject();

    cJSON* profile = cJSON_CreateObject();
    cJSON_AddStringToObject(profile, "name", "name");
    cJSON_AddStringToObject(profile, "description", "descr");


    cJSON *array = cJSON_CreateArray();
    cJSON_AddItemToArray(array, profile);
    cJSON_AddItemToObject(profiles, "profiles", array);

    try {
        loadProfilesFromJson(profiles);
    } catch (std::string &err) {
        std::cerr << "Test fail: loadProfilesFromJson(" <<
            cJSON_PrintUnformatted(profiles) << ") should not throw error"
                  << std::endl;
        exit(1);
    }

    cJSON_Delete(profiles);
}

static void test_find_profile(void)
{
    std::cout << "Testing find_profile" << std::endl;
    cJSON* profiles = cJSON_CreateObject();

    cJSON* profile = cJSON_CreateObject();
    cJSON_AddStringToObject(profile, "name", "name");
    cJSON_AddStringToObject(profile, "description", "descr");


    cJSON *array = cJSON_CreateArray();
    cJSON_AddItemToArray(array, profile);
    cJSON_AddItemToObject(profiles, "profiles", array);

    try {
        loadProfilesFromJson(profiles);
    } catch (std::string &err) {
        std::cerr << "Test fail: loadProfilesFromJson(" <<
            cJSON_PrintUnformatted(profiles) << ") should not throw error"
                  << std::endl;
        exit(1);
    }
    cJSON_Delete(profiles);

    if (find_profile("foobar") != NULL) {
        std::cerr << "Test fail: did not expect to find unknown profile"
                  << std::endl;
        exit(1);

    }

    profile_t *p = find_profile("name");
    if (p == NULL) {
        std::cerr << "Test fail: expected to find profile" << std::endl;
        exit(1);
    }

    if (p->refcount != 1) {
        std::cerr << "Test fail: expected refcount to be 1" << std::endl;
        exit(1);
    }

    profile_t *p2 = find_profile("name");
    if (p != p2 || p->refcount != 2) {
        std::cerr << "Test fail: expected refcount to be 2" << std::endl;
        exit(1);
    }
    release_profile(p);
    if (p->refcount != 1) {
        std::cerr << "Test fail: expected release to decrement refcount"
                  << std::endl;
        exit(1);
    }
    p->deleted = 1;
    p2 = find_profile("name");
    if (p2 != NULL) {
        std::cerr << "Test fail: did not expect to find deleted profile"
                  << std::endl;
        exit(1);
    }
    release_profile(p);
}

static void test_full_memcached_access(void)
{
    std::cout << "Testing full memcached access" << std::endl;
    cJSON* profiles = cJSON_CreateObject();

    cJSON* profile = cJSON_CreateObject();
    cJSON_AddStringToObject(profile, "name", "name");
    cJSON_AddStringToObject(profile, "description", "descr");

    cJSON* obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "allow", "all");
    cJSON_AddStringToObject(obj, "disallow", "none");
    cJSON_AddItemToObject(profile, "memcached", obj);

    cJSON *array = cJSON_CreateArray();
    cJSON_AddItemToArray(array, profile);
    cJSON_AddItemToObject(profiles, "profiles", array);

    try {
        loadProfilesFromJson(profiles);
    } catch (std::string &err) {
        std::cerr << "Test fail: loadProfilesFromJson(" <<
            cJSON_PrintUnformatted(profiles) << ") should not throw error"
                  << std::endl;
        exit(1);
    }

    profile_t *p = find_profile("name");
    for (int ii = 0; ii < 0x100; ++ii) {
        if (p->cmd[ii] == 0) {
            std::cerr << "Test fail: loadProfilesFromJson(" <<
                cJSON_PrintUnformatted(profiles)
                      << ") should allow all commands"
                      << std::endl;
            exit(1);
        }
    }

    cJSON_Delete(profiles);
}

static void test_no_memcached_access(void)
{
    std::cout << "Testing no memcached access" << std::endl;
    cJSON* profiles = cJSON_CreateObject();

    cJSON* profile = cJSON_CreateObject();
    cJSON_AddStringToObject(profile, "name", "name");
    cJSON_AddStringToObject(profile, "description", "descr");

    cJSON* obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "allow", "none");
    cJSON_AddStringToObject(obj, "disallow", "all");
    cJSON_AddItemToObject(profile, "memcached", obj);

    cJSON *array = cJSON_CreateArray();
    cJSON_AddItemToArray(array, profile);
    cJSON_AddItemToObject(profiles, "profiles", array);

    try {
        loadProfilesFromJson(profiles);
    } catch (std::string &err) {
        std::cerr << "Test fail: loadProfilesFromJson(" <<
            cJSON_PrintUnformatted(profiles) << ") should not throw error"
                  << std::endl;
        exit(1);
    }

    profile_t *p = find_profile("name");
    for (int ii = 0; ii < 0x100; ++ii) {
        if (p->cmd[ii] != 0) {
            std::cerr << "Test fail: loadProfilesFromJson(" <<
                cJSON_PrintUnformatted(profiles)
                      << ") should allow no commands"
                      << std::endl;
            exit(1);
        }
    }
    cJSON_Delete(profiles);
}

static void test_memcached_access_invalid_object(void)
{
    std::cout << "Testing invalid object type in disallow"
              << std::endl;
    cJSON* profiles = cJSON_CreateObject();

    cJSON* profile = cJSON_CreateObject();
    cJSON_AddStringToObject(profile, "name", "name");
    cJSON_AddStringToObject(profile, "description", "descr");


    cJSON *command = cJSON_CreateArray();
    cJSON_AddItemToArray(command, cJSON_CreateTrue());

    cJSON* obj = cJSON_CreateObject();
    cJSON_AddItemToObject(obj, "disallow", command);
    cJSON_AddStringToObject(obj, "allow", "all");
    cJSON_AddItemToObject(profile, "memcached", obj);

    cJSON *array = cJSON_CreateArray();
    cJSON_AddItemToArray(array, profile);
    cJSON_AddItemToObject(profiles, "profiles", array);

    try {
        loadProfilesFromJson(profiles);
        std::cerr << "Test fail: loadProfilesFromJson(" <<
            cJSON_PrintUnformatted(profiles) << ") should throw error"
                  << std::endl;
        exit(1);
    } catch (std::string &err) {
    }

    cJSON_Delete(profiles);
}

static void test_memcached_access_invalid_range(void)
{
    std::cout << "Testing invalid object type in disallow"
              << std::endl;
    cJSON* profiles = cJSON_CreateObject();

    cJSON* profile = cJSON_CreateObject();
    cJSON_AddStringToObject(profile, "name", "name");
    cJSON_AddStringToObject(profile, "description", "descr");


    cJSON *command = cJSON_CreateArray();
    cJSON_AddItemToArray(command, cJSON_CreateNumber(300));

    cJSON* obj = cJSON_CreateObject();
    cJSON_AddItemToObject(obj, "disallow", command);
    cJSON_AddStringToObject(obj, "allow", "all");
    cJSON_AddItemToObject(profile, "memcached", obj);

    cJSON *array = cJSON_CreateArray();
    cJSON_AddItemToArray(array, profile);
    cJSON_AddItemToObject(profiles, "profiles", array);

    try {
        loadProfilesFromJson(profiles);
        std::cerr << "Test fail: loadProfilesFromJson(" <<
            cJSON_PrintUnformatted(profiles) << ") should throw error"
                  << std::endl;
        exit(1);
    } catch (std::string &err) {
    }

    cJSON_Delete(profiles);
}


static void test_named_memcached_access(void)
{
    std::cout << "Testing named memcached access" << std::endl;
    cJSON* profiles = cJSON_CreateObject();

    cJSON* profile = cJSON_CreateObject();
    cJSON_AddStringToObject(profile, "name", "name");
    cJSON_AddStringToObject(profile, "description", "descr");

    cJSON* obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "allow", "noop");
    cJSON_AddStringToObject(obj, "disallow", "none");
    cJSON_AddItemToObject(profile, "memcached", obj);

    cJSON *array = cJSON_CreateArray();
    cJSON_AddItemToArray(array, profile);
    cJSON_AddItemToObject(profiles, "profiles", array);

    try {
        loadProfilesFromJson(profiles);
    } catch (std::string &err) {
        std::cerr << "Test fail: loadProfilesFromJson(" <<
            cJSON_PrintUnformatted(profiles) << ") should not throw error"
                  << std::endl;
        exit(1);
    }

    profile_t *p = find_profile("name");
    for (int ii = 0; ii < 0x100; ++ii) {
        if (ii == PROTOCOL_BINARY_CMD_NOOP) {
            if (p->cmd[ii] == 0) {
                std::cerr << "Test fail: loadProfilesFromJson(" <<
                    cJSON_PrintUnformatted(profiles)
                          << ") should only allow noop"
                          << std::endl;
                exit(1);
            }
        } else {
            if (p->cmd[ii] != 0) {
                std::cerr << "Test fail: loadProfilesFromJson(" <<
                    cJSON_PrintUnformatted(profiles)
                          << ") should only allow noop"
                          << std::endl;
                exit(1);
            }
        }
    }
    cJSON_Delete(profiles);
}

static void test_named_numeric_memcached_access(void)
{
    std::cout << "Testing named numeric memcached access" << std::endl;
    cJSON* profiles = cJSON_CreateObject();

    cJSON* profile = cJSON_CreateObject();
    cJSON_AddStringToObject(profile, "name", "name");
    cJSON_AddStringToObject(profile, "description", "descr");

    cJSON* obj = cJSON_CreateObject();
    cJSON_AddStringToObject(obj, "allow", "10");
    cJSON_AddStringToObject(obj, "disallow", "none");
    cJSON_AddItemToObject(profile, "memcached", obj);

    cJSON *array = cJSON_CreateArray();
    cJSON_AddItemToArray(array, profile);
    cJSON_AddItemToObject(profiles, "profiles", array);

    try {
        loadProfilesFromJson(profiles);
    } catch (std::string &err) {
        std::cerr << "Test fail: loadProfilesFromJson(" <<
            cJSON_PrintUnformatted(profiles) << ") should not throw error"
                  << std::endl;
        exit(1);
    }

    profile_t *p = find_profile("name");
    for (int ii = 0; ii < 0x100; ++ii) {
        if (ii == PROTOCOL_BINARY_CMD_NOOP) {
            if (p->cmd[ii] == 0) {
                std::cerr << "Test fail: loadProfilesFromJson(" <<
                    cJSON_PrintUnformatted(profiles)
                          << ") should only allow noop"
                          << std::endl;
                exit(1);
            }
        } else {
            if (p->cmd[ii] != 0) {
                std::cerr << "Test fail: loadProfilesFromJson(" <<
                    cJSON_PrintUnformatted(profiles)
                          << ") should only allow noop"
                          << std::endl;
                exit(1);
            }
        }
    }
    cJSON_Delete(profiles);
}

static void test_named_array_memcached_access(void)
{
    std::cout << "Testing multipe named commands memcached access" << std::endl;
    cJSON* profiles = cJSON_CreateObject();

    cJSON* profile = cJSON_CreateObject();
    cJSON_AddStringToObject(profile, "name", "name");
    cJSON_AddStringToObject(profile, "description", "descr");


    cJSON *command = cJSON_CreateArray();
    cJSON_AddItemToArray(command, cJSON_CreateString("noop"));
    cJSON_AddItemToArray(command, cJSON_CreateString("get"));
    cJSON_AddItemToArray(command, cJSON_CreateString("set"));
    cJSON_AddItemToArray(command, cJSON_CreateString("2"));
    cJSON_AddItemToArray(command, cJSON_CreateNumber(3));

    cJSON* obj = cJSON_CreateObject();
    cJSON_AddItemToObject(obj, "allow", command);
    cJSON_AddStringToObject(obj, "disallow", "none");
    cJSON_AddItemToObject(profile, "memcached", obj);

    cJSON *array = cJSON_CreateArray();
    cJSON_AddItemToArray(array, profile);
    cJSON_AddItemToObject(profiles, "profiles", array);

    try {
        loadProfilesFromJson(profiles);
    } catch (std::string &err) {
        std::cerr << "Test fail: loadProfilesFromJson(" <<
            cJSON_PrintUnformatted(profiles) << ") should not throw error"
                  << std::endl;
        exit(1);
    }

    profile_t *p = find_profile("name");
    for (int ii = 0; ii < 0x100; ++ii) {
        switch (ii) {
        case PROTOCOL_BINARY_CMD_NOOP:
        case PROTOCOL_BINARY_CMD_GET:
        case PROTOCOL_BINARY_CMD_SET:
        case 3:
        case 2:
            if (p->cmd[ii] == 0) {
                std::cerr << "Test fail: loadProfilesFromJson(" <<
                    cJSON_PrintUnformatted(profiles)
                          << ") should allow " << ii
                          << std::endl;
                exit(1);
            }
            break;
        default:
            if (p->cmd[ii] != 0) {
                std::cerr << "Test fail: loadProfilesFromJson(" <<
                    cJSON_PrintUnformatted(profiles)
                          << ") should not allow " << ii
                          << std::endl;
                exit(1);
            }
        }
    }

    cJSON_Delete(profiles);
}

static void test_disable_named_array_memcached_access(void)
{
    std::cout << "Testing disable multipe named commands memcached access"
              << std::endl;
    cJSON* profiles = cJSON_CreateObject();

    cJSON* profile = cJSON_CreateObject();
    cJSON_AddStringToObject(profile, "name", "name");
    cJSON_AddStringToObject(profile, "description", "descr");


    cJSON *command = cJSON_CreateArray();
    cJSON_AddItemToArray(command, cJSON_CreateString("noop"));
    cJSON_AddItemToArray(command, cJSON_CreateString("get"));
    cJSON_AddItemToArray(command, cJSON_CreateString("set"));
    cJSON_AddItemToArray(command, cJSON_CreateString("2"));
    cJSON_AddItemToArray(command, cJSON_CreateNumber(3));

    cJSON* obj = cJSON_CreateObject();
    cJSON_AddItemToObject(obj, "disallow", command);
    cJSON_AddStringToObject(obj, "allow", "all");
    cJSON_AddItemToObject(profile, "memcached", obj);

    cJSON *array = cJSON_CreateArray();
    cJSON_AddItemToArray(array, profile);
    cJSON_AddItemToObject(profiles, "profiles", array);

    try {
        loadProfilesFromJson(profiles);
    } catch (std::string &err) {
        std::cerr << "Test fail: loadProfilesFromJson(" <<
            cJSON_PrintUnformatted(profiles) << ") should not throw error"
                  << std::endl;
        exit(1);
    }

    profile_t *p = find_profile("name");
    for (int ii = 0; ii < 0x100; ++ii) {
        switch (ii) {
        case PROTOCOL_BINARY_CMD_NOOP:
        case PROTOCOL_BINARY_CMD_GET:
        case PROTOCOL_BINARY_CMD_SET:
        case 3:
        case 2:
            if (p->cmd[ii] != 0) {
                std::cerr << "Test fail: loadProfilesFromJson(" <<
                    cJSON_PrintUnformatted(profiles)
                          << ") should not allow " << ii
                          << std::endl;
                exit(1);
            }
            break;
        default:
            if (p->cmd[ii] == 0) {
                std::cerr << "Test fail: loadProfilesFromJson(" <<
                    cJSON_PrintUnformatted(profiles)
                          << ") should allow command " << ii
                          << std::endl;
                exit(1);
            }
        }
    }

    cJSON_Delete(profiles);
}




typedef void (*testcase_t)(void);

int main(int argc, char **argv) {
    std::list<testcase_t> tests;
    tests.push_back(test_null_json);
    tests.push_back(test_missing_profiles);
    tests.push_back(test_invalid_profiles);
    tests.push_back(test_empty_profiles);
    tests.push_back(test_profile_missing_name);
    tests.push_back(test_profile_missing_description);
    tests.push_back(test_profile_missing_memcached);
    tests.push_back(test_find_profile);
    tests.push_back(test_full_memcached_access);
    tests.push_back(test_no_memcached_access);
    tests.push_back(test_memcached_access_invalid_object);
    tests.push_back(test_memcached_access_invalid_range);
    tests.push_back(test_named_memcached_access);
    tests.push_back(test_named_numeric_memcached_access);
    tests.push_back(test_named_array_memcached_access);
    tests.push_back(test_disable_named_array_memcached_access);

    std::list<testcase_t>::iterator iter;
    for (iter = tests.begin(); iter != tests.end(); ++iter) {
        initialize_profiles();
        (*iter)();
        destroy_profiles();

    }

    return 0;
}
