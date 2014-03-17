/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2012 Couchbase, Inc
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#ifndef SRC_COUCH_KVSTORE_COUCH_NOTIFIER_H_
#define SRC_COUCH_KVSTORE_COUCH_NOTIFIER_H_ 1

#include "config.h"

#include <event.h>

#include <list>
#include <map>
#include <queue>
#include <string>
#include <vector>

#include "callbacks.h"
#include "configuration.h"
#include "kvstore.h"
#include "mutex.h"

/*
 * libevent2 define evutil_socket_t so that it'll automagically work
 * on windows
 */
#ifndef evutil_socket_t
#define evutil_socket_t int
#endif

/*
 * Current CouchNotifier commands
 */
enum notifierCmd {
    update_vbucket_cmd = 1,
    flush_vbucket_cmd,
    del_vbucket_cmd,
    select_bucket_cmd,
    unknown_cmd
};
const int MAX_NUM_NOTIFIER_CMD = unknown_cmd;

class Buffer {
public:
    char *data;
    size_t size;
    size_t avail;
    size_t curr;

    Buffer() : data(NULL), size(0), avail(0), curr(0) { /* EMPTY */ }

    Buffer(size_t s) : data((char*)malloc(s)), size(s), avail(s), curr(0)
    {
        cb_assert(data != NULL);
    }

    Buffer(const Buffer &other) :
        data((char*)malloc(other.size)), size(other.size), avail(other.avail),
        curr(other.curr)
    {
        cb_assert(data != NULL);
        memcpy(data, other.data, size);
    }

    ~Buffer() {
        free(data);
    }

    bool grow() {
        return grow(8192);
    }

    bool grow(size_t minFree) {
        if (minFree == 0) {
            // no minimum size requested, just ensure that there is at least
            // one byte there...
            minFree = 1;
        }

        if (size - avail < minFree) {
            size_t next = size ? size << 1 : 8192;
            char *ptr;

            while ((next - avail) < minFree) {
                next <<= 1;
            }

            ptr = (char*)realloc(data, next);
            if (ptr == NULL) {
                return false;
            }
            data = ptr;
            size = next;
        }

        return true;
    }
};

class BinaryPacketHandler;
class SelectBucketResponseHandler;

#define VB_NO_CHANGE 0x00
#define VB_STATE_CHANGED 0x01
#define VB_CHECKPOINT_CHANGED 0x02
#define VB_COMPACTION_DONE 0x04
#define VB_COMPACT_OPENDB_ERROR 0x08
#define VB_COMPACT_RENAME_ERROR 0x0C

class VBStateNotification {
public:
    VBStateNotification(uint64_t _chk, uint32_t _state,
                        uint32_t _type, uint16_t _vbucket) :
        checkpoint(_chk), state(_state),
        updateType(_type), vbucket(_vbucket) { }

    uint64_t checkpoint;
    uint32_t state;
    uint32_t updateType;
    uint16_t vbucket;
};

class CouchNotifier {
public:
    static void deleteNotifier() {
        LockHolder lh(initMutex);
        if (--refCount == 0) {
            // no bucket is referencing CouchNotifier instances
            // we can safely free all now
            std::map<std::string, CouchNotifier *>::iterator it;
            for (it = instances.begin(); it != instances.end(); ++it) {
                delete it->second;
            }
            instances.clear();
        }
    }
    static CouchNotifier *create(EPStats &s, Configuration &c) {
        LockHolder lh(initMutex);
        ++refCount;
        std::string bucketName = c.getCouchBucket();
        if (instances.find(bucketName) == instances.end()) {
            instances[bucketName] = new CouchNotifier(s, c);
        }
        return instances[bucketName];
    }
    void flush(Callback<bool> &cb);
    void delVBucket(uint16_t vb, Callback<bool> &cb);

    void notify_update(const VBStateNotification &vbs,
                       uint64_t file_version,
                       uint64_t header_offset,
                       Callback<uint16_t> &cb);

    void notify_headerpos_update(uint16_t vbucket,
                                 uint64_t file_version,
                                 uint64_t header_offset,
                                 Callback<uint16_t> &cb) {
        VBStateNotification vbs(0, 0, VB_NO_CHANGE, vbucket);
        notify_update(vbs, file_version, header_offset, cb);
    }

    void addStats(const std::string &prefix,
                  ADD_STAT add_stat,
                  const void *c);

protected:
    friend class SelectBucketResponseHandler;

private:
    CouchNotifier(EPStats &st, Configuration &config);
    ~CouchNotifier() { }
    void selectBucket(void);
    void reschedule(std::list<BinaryPacketHandler*> &packets);
    void resetConnection();

    void sendSingleChunk(const char *ptr, size_t nb);
    void sendCommand(BinaryPacketHandler *rh);
    bool processInput();
    void maybeProcessInput();
    void wait();
    bool waitOnce();

    void handleResponse(protocol_binary_response_header *res);

    bool waitForWritable();
    bool waitForReadable(bool tryOnce = false);

    bool connect();
    void ensureConnection(void);
    int  commandId(uint8_t opcode);
    const char *cmdId2str(int id);

    evutil_socket_t sock;

    EPStats &stats;
    std::string bucketName;
    size_t responseTimeOut;
    size_t reconnectSleepTime;
    size_t port;
    std::string host;
    bool allowDataLoss;
    bool configurationError;

    uint64_t seqno;
    Buffer input;

    /**
     * The current command in transit (set to 0xff when no command is in
     * transit. Please note that this is read and written completely
     * dirty, so you may not trust the value ;)
     */
    volatile uint8_t currentCommand;
    volatile uint8_t lastSentCommand;
    volatile uint8_t lastReceivedCommand;

    const char *cmd2str(uint8_t cmd);

    /**
     * Structure used "per command"
     */
    class CommandStats {
    public:
        CommandStats() : numSent(0), numSuccess(0),
                         numImplicit(), numError(0) { }
        volatile size_t numSent;
        volatile size_t numSuccess;
        volatile size_t numImplicit;
        volatile size_t numError;

        void addStat(const std::string &prefix, const char *nm, size_t val,
                     ADD_STAT add_stat, const void *c) {
            std::stringstream name;
            name << prefix << ":" << nm;
            std::stringstream value;
            value << val;
            std::string n = name.str();

            EventuallyPersistentEngine *e = ObjectRegistry::onSwitchThread(NULL, true);
            add_stat(n.data(),
                    static_cast<uint16_t> (n.length()),
                    value.str().data(),
                    static_cast<uint32_t> (value.str().length()), c);
            ObjectRegistry::onSwitchThread(e);
        }

        void addStats(const std::string &prefix,
                      const char *cmd,
                      ADD_STAT add_stat,
                      const void *c) {
            if (numSent > 0 || numSuccess > 0 ||
                numImplicit != 0 || numError != 0)
            {
                if (strcmp(cmd, "unknown") == 0) {
                    abort();
                };

                std::stringstream name;
                name << prefix << ":" << cmd;
                addStat(name.str(), "sent", numSent, add_stat, c);
                addStat(name.str(), "success", numSuccess, add_stat, c);
                addStat(name.str(), "implicit", numImplicit, add_stat, c);
                addStat(name.str(), "error", numError, add_stat, c);
            }
        }
    } commandStats[MAX_NUM_NOTIFIER_CMD];

    Mutex mutex;
    std::list<BinaryPacketHandler*> responseHandler;
    bool connected;
    bool inSelectBucket;

    struct msghdr sendMsg;
    struct iovec sendIov[IOV_MAX];
    int numiovec;

    static Mutex initMutex;
    static std::map<std::string, CouchNotifier *> instances;
    static uint16_t refCount;
};

#endif  // SRC_COUCH_KVSTORE_COUCH_NOTIFIER_H_
