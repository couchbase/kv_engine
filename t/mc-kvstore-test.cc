#include "mc-kvstore/mc-kvstore.cc"
#include "item.hh"

extern "C" {
static const char *get_name(void) {
    return "blackhole";
}
static void logger_log(EXTENSION_LOG_LEVEL severity, const void* client_cookie,
        const char *fmt, ...) {
    (void)severity;
    (void)client_cookie;
    (void)fmt;
}

static EXTENSION_LOGGER_DESCRIPTOR descriptor;
}

EXTENSION_LOGGER_DESCRIPTOR *getLogger() {
    return &descriptor;
}


class MCKVStoreTestEnvironment {
public:
   MCKVStoreTestEnvironment() {
       config.setCouchHost(getenv("COUCH_HOST"));
   }
   Configuration config;
};

EventuallyPersistentEngine *noEngine = 0;
EPStats* noStats = 0;

MCKVStore::MCKVStore(MCKVStoreTestEnvironment &mock) :
    KVStore(), stats(*noStats), intransaction(false), mc(NULL),
            config(mock.config), engine(*noEngine) {
    open();
}

static void testSet(MCKVStore &kvstore) {
   std::list<RememberingCallback<mutation_result>*> callbacks;
   kvstore.begin();
   for (int ii = 0; ii < 1000; ++ii) {
      RememberingCallback<mutation_result>* cb = new RememberingCallback<mutation_result>;
      std::stringstream ss;
      ss << "key_" << ii;
      Item itm(ss.str().c_str(), ss.str().length(), 0, NULL, 0);
      kvstore.set(itm, 0, *cb);
      callbacks.push_back(cb);
   }
   kvstore.commit();
   std::list<RememberingCallback<mutation_result>*>::iterator iter;
   for (iter = callbacks.begin(); iter != callbacks.end(); ++iter) {
      delete *iter;
   }
}

static void testGet(MCKVStore &kvstore) {
    RememberingCallback<GetValue> cb;
    kvstore.get("foo", 0, 0, 0, cb);
    cb.waitForValue();
}

static void testDel(MCKVStore &kvstore) {
    RememberingCallback<int> cb;
    Item itm("foo", 3, 0, NULL, 0);
    kvstore.del(itm, 0, 0, cb);
    cb.waitForValue();
}

static void testDelVucket(MCKVStore &kvstore) {
    kvstore.delVBucket(0, 0);
}

static void testDump(MCKVStore &kvstore) {
    shared_ptr<RememberingCallback<GetValue> > cb(new RememberingCallback<GetValue>()); 
    kvstore.dump(cb);
}

#if 0
static void testDump(uint16_t vb, MCKVStore &kvstore) {
    RememberingCallback<GetValue> cb;
    kvstore.dump(vb, cb);
}
#endif

int main(int argc, char **argv) {
   (void)argc; (void)argv;
    descriptor.get_name = get_name;
    descriptor.log = logger_log;
    putenv(strdup("ALLOW_NO_STATS_UPDATE=foo"));

    if (getenv("COUCH_HOST") == NULL) {
       std::cout << "Test skipped.. Set COUCH_HOST to the host to test" << std::endl;
       return 0;
    }

    MCKVStoreTestEnvironment env;
    MCKVStore kvstore(env);

    // kvstore.begin();
    // kvstore.commit();

    kvstore.listPersistedVbuckets();

    vbucket_map_t map;
    for (uint16_t ii = 0; ii < 1024; ++ii) {
       std::pair<uint16_t, uint16_t> vb(ii, 0);
       vbucket_state vb_state;
       vb_state.state = vbucket_state_active;
       vb_state.checkpointId = 0;
       map[vb] = vb_state;
    }

    assert(kvstore.snapshotVBuckets(map));
    // @todo verify..
    kvstore.listPersistedVbuckets();

    testDump(kvstore);

    assert(kvstore.getNumShards() == 1);
    testSet(kvstore);
    testDump(kvstore);
    testGet(kvstore);
    testDel(kvstore);
    testGet(kvstore);
    testDelVucket(kvstore);



    return 0;
}
