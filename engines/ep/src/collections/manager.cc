/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2017 Couchbase, Inc
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

#include "collections/manager.h"
#include "bucket_logger.h"
#include "collections/manifest.h"
#include "ep_engine.h"
#include "kv_bucket.h"
#include "statwriter.h"
#include "string_utils.h"
#include "vb_visitors.h"
#include "vbucket.h"

#include <spdlog/fmt/ostr.h>

Collections::Manager::Manager() {
}

cb::engine_error Collections::Manager::update(KVBucket& bucket,
                                              cb::const_char_buffer manifest) {
    // Get upgrade access to the manifest for the initial part of the update
    // This gives shared access (other readers allowed) but would block other
    // attempts to get upgrade access.
    auto current = currentManifest.ulock();

    std::unique_ptr<Manifest> newManifest;
    // Construct a newManifest (will throw if JSON was illegal)
    try {
        newManifest = std::make_unique<Manifest>(
                manifest,
                bucket.getEPEngine().getConfiguration().getScopesMaxSize(),
                bucket.getEPEngine()
                        .getConfiguration()
                        .getCollectionsMaxSize());
    } catch (std::exception& e) {
        EP_LOG_WARN(
                "Collections::Manager::update can't construct manifest "
                "e.what:{}",
                e.what());
        return cb::engine_error(
                cb::engine_errc::invalid_arguments,
                "Collections::Manager::update manifest json invalid:" +
                        cb::to_string(manifest));
    }

    // If the new manifest has a non zero uid, try to apply it
    if (newManifest->getUid() != 0) {
        // However expect it to be increasing
        if (newManifest->getUid() <= current->getUid()) {
            // Bad - newManifest has a equal/lower UID
            EP_LOG_WARN(
                    "Collections::Manager::update the new manifest has "
                    "UID < current manifest UID. Current UID:{}, New "
                    "Manifest:{}",
                    current->getUid(),
                    cb::to_string(manifest));
            return cb::engine_error(
                    cb::engine_errc::out_of_range,
                    "Collections::Manager::update new UID cannot "
                    "be lower than existing UID");
        }

        auto updated = updateAllVBuckets(bucket, *newManifest);
        if (updated.is_initialized()) {
            auto rolledback = updateAllVBuckets(bucket, *current);
            return cb::engine_error(
                    cb::engine_errc::cannot_apply_collections_manifest,
                    "Collections::Manager::update aborted on " +
                            updated->to_string() + " and rolled-back success:" +
                            std::to_string(!rolledback.is_initialized()) +
                            ", cannot apply:" + cb::to_string(manifest));
        }

        // Now switch to write locking and change the manifest. The lock is
        // released after this statement.
        *current.moveFromUpgradeToWrite() = std::move(*newManifest);
    } else if (*newManifest != *current) {
        // The new manifest has a uid:0, we tolerate an update where current and
        // new have a uid:0, but expect that the manifests are equal.
        // So this else case catches when the manifests aren't equal
        EP_LOG_WARN(
                "Collections::Manager::update error. The new manifest does not "
                "match and we think it should. current:{}, new:{}",
                current->toJson(),
                cb::to_string(manifest));
        return cb::engine_error(
                cb::engine_errc::cannot_apply_collections_manifest,
                "Collections::Manager::update failed. Manifest mismatch");
    }
    return cb::engine_error(cb::engine_errc::success,
                            "Collections::Manager::update");
}

boost::optional<Vbid> Collections::Manager::updateAllVBuckets(
        KVBucket& bucket, const Manifest& newManifest) {
    for (Vbid::id_type i = 0; i < bucket.getVBuckets().getSize(); i++) {
        auto vb = bucket.getVBuckets().getBucket(Vbid(i));

        if (vb && vb->getState() == vbucket_state_active) {
            if (!vb->updateFromManifest(newManifest)) {
                return vb->getId();
            }
        }
    }
    return {};
}

std::pair<cb::mcbp::Status, std::string> Collections::Manager::getManifest()
        const {
    return {cb::mcbp::Status::Success, currentManifest.rlock()->toJson()};
}

bool Collections::Manager::validateGetCollectionIDPath(
        const std::string& path) {
    return std::count(path.begin(), path.end(), '.') == 1;
}

bool Collections::Manager::validateGetScopeIDPath(const std::string& path) {
    return std::count(path.begin(), path.end(), '.') <= 1;
}

cb::EngineErrorGetCollectionIDResult Collections::Manager::getCollectionID(
        cb::const_char_buffer path) const {
    auto sPath = cb::to_string(path);

    if (!validateGetCollectionIDPath(sPath)) {
        return {cb::engine_errc::invalid_arguments, 0, 0};
    }

    auto current = currentManifest.rlock();
    auto scope = current->getScopeID(sPath);
    if (!scope) {
        return {cb::engine_errc::unknown_scope, current->getUid(), 0};
    }

    auto collection = current->getCollectionID(scope.get(), sPath);
    if (!collection) {
        return {cb::engine_errc::unknown_collection, current->getUid(), 0};
    }

    return {cb::engine_errc::success, current->getUid(), collection.get()};
}

cb::EngineErrorGetScopeIDResult Collections::Manager::getScopeID(
        cb::const_char_buffer path) const {
    auto sPath = cb::to_string(path);
    if (!validateGetScopeIDPath(sPath)) {
        return {cb::engine_errc::invalid_arguments, 0, 0};
    }

    auto current = currentManifest.rlock();
    auto scope = current->getScopeID(sPath);
    if (!scope) {
        return {cb::engine_errc::unknown_scope, current->getUid(), 0};
    }

    return {cb::engine_errc::success, current->getUid(), scope.get()};
}

std::pair<uint64_t, boost::optional<ScopeID>> Collections::Manager::getScopeID(
        const DocKey& key) const {
    // 'shortcut' For the default collection, just return the default scope.
    // If the default collection was deleted the vbucket will have the final say
    // but for this interface allow this without taking the rlock.
    if (key.getCollectionID().isDefaultCollection()) {
        // Allow the default collection in the default scope...
        return std::make_pair<uint64_t, boost::optional<ScopeID>>(
                0, ScopeID{ScopeID::Default});
    }

    auto current = currentManifest.rlock();
    return std::make_pair<uint64_t, boost::optional<ScopeID>>(
            current->getUid(), current->getScopeID(key));
}

void Collections::Manager::update(VBucket& vb) const {
    // Lock manager updates
    currentManifest.withRLock(
            [&vb](const auto& manifest) { vb.updateFromManifest(manifest); });
}

// This method is really to aid development and allow the dumping of the VB
// collection data to the logs.
void Collections::Manager::logAll(KVBucket& bucket) const {
    EP_LOG_INFO("{}", *this);
    for (Vbid::id_type i = 0; i < bucket.getVBuckets().getSize(); i++) {
        Vbid vbid = Vbid(i);
        auto vb = bucket.getVBuckets().getBucket(vbid);
        if (vb) {
            EP_LOG_INFO("{}: {} {}",
                        vbid,
                        VBucket::toString(vb->getState()),
                        vb->lockCollections());
        }
    }
}

void Collections::Manager::addCollectionStats(const void* cookie,
                                              const AddStatFn& add_stat) const {
    currentManifest.rlock()->addCollectionStats(cookie, add_stat);
}

void Collections::Manager::addScopeStats(const void* cookie,
                                         const AddStatFn& add_stat) const {
    currentManifest.rlock()->addScopeStats(cookie, add_stat);
}

/**
 * Perform actions for a completed warmup - currently check if any
 * collections are 'deleting' and require erasing retriggering.
 */
void Collections::Manager::warmupCompleted(KVBucket& bucket) const {
    for (Vbid::id_type i = 0; i < bucket.getVBuckets().getSize(); i++) {
        Vbid vbid = Vbid(i);
        auto vb = bucket.getVBuckets().getBucket(vbid);
        if (vb) {
            if (vb->lockCollections().isDropInProgress()) {
                Collections::VB::Flush::triggerPurge(vbid, bucket);
            }
        }
    }
}

class CollectionCountVBucketVisitor : public VBucketVisitor {
public:
    void visitBucket(const VBucketPtr& vb) override {
        if (vb->getState() == vbucket_state_active) {
            vb->lockCollections().updateSummary(summary);
        }
    }
    Collections::Summary summary;
};

class CollectionDetailedVBucketVisitor : public VBucketVisitor {
public:
    CollectionDetailedVBucketVisitor(const void* c, const AddStatFn& a)
        : cookie(c), add_stat(a) {
    }

    void visitBucket(const VBucketPtr& vb) override {
        success = vb->lockCollections().addCollectionStats(
                          vb->getId(), cookie, add_stat) ||
                  success;
    }

    bool getSuccess() const {
        return success;
    }

private:
    const void* cookie;
    AddStatFn add_stat;
    bool success = true;
};

class ScopeDetailedVBucketVisitor : public VBucketVisitor {
public:
    ScopeDetailedVBucketVisitor(const void* c, const AddStatFn& a)
        : cookie(c), add_stat(a) {
    }

    void visitBucket(const VBucketPtr& vb) override {
        success = vb->lockCollections().addScopeStats(
                          vb->getId(), cookie, add_stat) ||
                  success;
    }

    bool getSuccess() const {
        return success;
    }

private:
    const void* cookie;
    AddStatFn add_stat;
    bool success = true;
};

// collections-details
//   - return top level stats (manager/manifest)
//   - iterate vbuckets returning detailed VB stats
// collections-details n
//   - return detailed VB stats for n only
// collections
//   - return top level stats (manager/manifest)
//   - return per collection item counts from all active VBs
ENGINE_ERROR_CODE Collections::Manager::doCollectionStats(
        KVBucket& bucket,
        const void* cookie,
        const AddStatFn& add_stat,
        const std::string& statKey) {
    bool success = true;
    if (cb_isPrefix(statKey, "collections-details")) {
        // VB maybe encoded in statKey
        auto pos = statKey.find_first_of(" ");
        if (pos != std::string::npos) {
            try {
                Vbid vbid = Vbid(std::stoi(statKey.substr(pos)));
                VBucketPtr vb = bucket.getVBucket(vbid);
                if (vb) {
                    success = vb->lockCollections().addCollectionStats(
                            vbid, cookie, add_stat);
                } else {
                    return ENGINE_NOT_MY_VBUCKET;
                }
            } catch (const std::exception& e) {
                EP_LOG_WARN(
                        "Collections::Manager::doStats failed to build "
                        "stats for {} exception:{}",
                        statKey.substr(pos),
                        e.what());
                return ENGINE_EINVAL;
            }
        } else {
            bucket.getCollectionsManager().addCollectionStats(cookie, add_stat);
            CollectionDetailedVBucketVisitor visitor(cookie, add_stat);
            bucket.visit(visitor);
            success = visitor.getSuccess();
        }
    } else {
        // Do the high level stats (includes global count)
        bucket.getCollectionsManager().addCollectionStats(cookie, add_stat);
        CollectionCountVBucketVisitor visitor;
        bucket.visit(visitor);
        for (const auto& entry : visitor.summary) {
            try {
                const int bsize = 512;
                char buffer[bsize];
                checked_snprintf(buffer,
                                 bsize,
                                 "collection:%s:items",
                                 entry.first.to_string().c_str());
                add_casted_stat(buffer, entry.second, add_stat, cookie);
            } catch (const std::exception& e) {
                EP_LOG_WARN(
                        "Collections::Manager::doStats failed to build stats: "
                        "{}",
                        e.what());
                success = false;
            }
        }
    }

    return success ? ENGINE_SUCCESS : ENGINE_FAILED;
}

// scopes-details
//   - return top level stats (manager/manifest)
//   - iterate vbucket returning detailed VB stats
// scopes-details n
//   - return detailed VB stats for n only
// scopes
//   - return top level stats (manager/manifest)
//   - return number of collections from all active VBs
ENGINE_ERROR_CODE Collections::Manager::doScopeStats(
        KVBucket& bucket,
        const void* cookie,
        const AddStatFn& add_stat,
        const std::string& statKey) {
    bool success = true;
    if (cb_isPrefix(statKey, "scopes-details")) {
        // VB may be encoded in statKey
        auto pos = statKey.find_first_of(" ");
        if (pos != std::string::npos) {
            try {
                Vbid vbid = Vbid(std::stoi(statKey.substr(pos)));
                VBucketPtr vb = bucket.getVBucket(vbid);
                if (vb) {
                    success = vb->lockCollections().addScopeStats(
                            vbid, cookie, add_stat);
                } else {
                    return ENGINE_NOT_MY_VBUCKET;
                }
            } catch (const std::exception& e) {
                EP_LOG_WARN(
                        "Collections::Manager::doScopeStats failed to "
                        "build stats for {} exception:{}",
                        statKey.substr(pos),
                        e.what());
                return ENGINE_EINVAL;
            }
        } else {
            bucket.getCollectionsManager().addScopeStats(cookie, add_stat);
            ScopeDetailedVBucketVisitor visitor(cookie, add_stat);
            bucket.visit(visitor);
            success = visitor.getSuccess();
        }
    } else {
        // Do the high level stats (includes number of collections)
        bucket.getCollectionsManager().addScopeStats(cookie, add_stat);
    }

    return success ? ENGINE_SUCCESS : ENGINE_FAILED;
}

void Collections::Manager::dump() const {
    std::cerr << *this;
}

std::ostream& Collections::operator<<(std::ostream& os,
                                      const Collections::Manager& manager) {
    os << "Collections::Manager current:" << *manager.currentManifest.rlock()
       << "\n";
    return os;
}
