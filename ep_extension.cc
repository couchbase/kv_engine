/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "ep_extension.h"
#include <memcached/extension.h>
#include <memcached/util.h>

#define ITEM_LOCK_TIMEOUT       15    /* 15 seconds */
#define MAX_KEY_LEN             250   /* maximum permissible key length */


static GetlExtension* getExtension(const void* cookie)
{
    return reinterpret_cast<GetlExtension*>(const_cast<void *>(cookie));
}

extern "C" {

    typedef bool (*RESPONSE_HANDLER_T)(const void *, int , const char *);
    static const char *ext_get_name(const void *cmd_cookie) {
        (void) cmd_cookie;
        return "getl";
    }

    static bool ext_execute(const void *cmd_cookie, const void *cookie,
            int argc, token_t *argv,
            RESPONSE_HANDLER_T response_handler) {
        (void) cmd_cookie;

        bool ret = getExtension(cmd_cookie)->executeGetl(argc, argv,
                                                         (void *)cookie,
                                                         response_handler);

        return ret;
    }

    static bool ext_accept(const void *cmd_cookie, void *cookie,
            int argc, token_t *argv, size_t *ndata,
            char **ptr) {
        (void) cmd_cookie;
        (void) cookie;
        (void) argc;
        (void) ndata;
        (void) ptr;
        return argc >= 1 && strncmp(argv[0].value, "getl", argv[0].length) == 0;
    }

    static void ext_abort(const void *cmd_cookie, const void *cookie) {
        (void) cmd_cookie;
        (void) cookie;
    }

}  /* extern C */

GetlExtension::GetlExtension(EventuallyPersistentStore *kvstore,
                             GET_SERVER_API get_server_api):
    backend(kvstore)
{
    serverApi = get_server_api();
}

void GetlExtension::initialize()
{
    if (serverApi != NULL) {
        EXTENSION_ASCII_PROTOCOL_DESCRIPTOR *ptr = this;
        get_name = ext_get_name;
        accept = ext_accept;
        execute = ext_execute;
        abort = ext_abort;
        cookie = ptr;
        serverApi->extension->register_extension(EXTENSION_ASCII_PROTOCOL, ptr);

        getLogger()->log(EXTENSION_LOG_INFO, NULL, "Loaded extension: getl\n");
    }
}

bool GetlExtension::executeGetl(int argc, token_t *argv, void *response_cookie,
                                RESPONSE_HANDLER_T response_handler)
{
    uint32_t lockTimeout = ITEM_LOCK_TIMEOUT;

    if (argc == 3) {
        if (!safe_strtoul(argv[2].value, &lockTimeout) ||
                lockTimeout > (ITEM_LOCK_TIMEOUT * 2)) {
            lockTimeout = ITEM_LOCK_TIMEOUT;
        }
    } else if (argc != 2) {
        return (response_handler(response_cookie,
                                 sizeof("CLIENT_ERROR\r\n") - 1,
                                 "CLIENT_ERROR\r\n"));
    }

    std::string k(argv[1].value, argv[1].length);
    RememberingCallback<GetValue> getCb;

    // TODO:  Get vbucket ID here.
    bool gotLock = backend->getLocked(k, 0, getCb,
            serverApi->core->get_current_time(),
            lockTimeout);

    Item *item = NULL;
    bool ret = true;

    getCb.waitForValue();
    if (getCb.val.getStatus() == ENGINE_SUCCESS) {
        item = getCb.val.getValue();
        std::stringstream strm;

        strm << "VALUE " << item->getKey() << " " << item->getFlags()
             << " " << item->getNBytes() -2 << " " << item->getCas() << "\r\n";

        std::string strVal = strm.str();
        size_t len = strVal.length();

        ret = response_handler(response_cookie, static_cast<int>(len),
                               strVal.c_str())
            && response_handler(response_cookie, item->getNBytes(),
                                item->getData())
            && response_handler(response_cookie, 5, "END\r\n");

    } else if (!gotLock){
        ret = response_handler(response_cookie,
                               sizeof("LOCK_ERROR\r\n") - 1, "LOCK_ERROR\r\n");
    } else {
        ret = response_handler(response_cookie,
                               sizeof("NOT_FOUND\r\n") - 1, "NOT_FOUND\r\n");
    }

    if (item != NULL) delete item;

    return ret;
}
