/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include <nlohmann/json.hpp>
#include <optional>

#include "input_couchfile.h"
#include "kvstore/storage_common/storage_common/local_doc_constants.h"
#include "output_couchfile.h"

namespace Collections {

InputCouchFile::InputCouchFile(OptionsSet options, const std::string& filename)
    : CouchFile(options, filename, 0) {
}

InputCouchFile::PreflightStatus InputCouchFile::preflightChecks(
        std::ostream& os) const {
    if (!doesLocalDocumentExist(LocalDocKey::vbstate)) {
        os << "filename:" << filename
           << " does not have a \"_local/vbstate\" document\n";
        return PreflightStatus::InputFileCannotBeProcessed;
    }

    bool partial = isPartiallyNamespaced();
    bool complete = isCompletelyNamespaced();

    if (partial && complete) {
        os << "filename:" << filename
           << " is showing both partial and complete\n";
        return PreflightStatus::UpgradeCompleteAndPartial;
    }

    if (partial) {
        os << "filename:" << filename << " is already partially processed\n";
        return PreflightStatus::UpgradePartial;
    }

    if (complete) {
        os << "filename:" << filename << " is already processed\n";
        return PreflightStatus::UpgradeComplete;
    }

    return PreflightStatus::ReadyForUpgrade;
}

struct UpgradeCouchFileContext {
    OutputCouchFile& output;
};

static int upgradeCallback(Db* db, DocInfo* docinfo, void* ctx) {
    Doc* doc = nullptr;
    if (!docinfo) {
        throw std::runtime_error(
                "InputCouchFile::upgradeCallback with null docinfo");
    }

    auto errcode = couchstore_open_doc_with_docinfo(
            db, docinfo, &doc, DECOMPRESS_DOC_BODIES);

    if (errcode && errcode != COUCHSTORE_ERROR_DOC_NOT_FOUND) {
        throw std::runtime_error(
                "InputCouchFile::upgradeCallback "
                "couchstore_open_doc_with_docinfo errcode:" +
                std::to_string(errcode));
    }
    auto* context = reinterpret_cast<UpgradeCouchFileContext*>(ctx);
    context->output.processDocument(doc, *docinfo);
    return 0;
}

void InputCouchFile::upgrade(OutputCouchFile& output) const {
    UpgradeCouchFileContext context{output};

    auto errcode = couchstore_all_docs(
            db, nullptr, 0 /*no options*/, &upgradeCallback, &context);
    if (errcode) {
        throw std::runtime_error(
                "InputCouchFile::upgrade couchstore_all_docs errcode:" +
                std::to_string(errcode));
    }

    output.commit();
}

bool InputCouchFile::doesLocalDocumentExist(
        std::string_view documentName) const {
    return openLocalDocument(documentName) != nullptr;
}

std::string InputCouchFile::getLocalDocument(
        std::string_view documentName) const {
    auto result = openLocalDocument(documentName);
    if (!result) {
        throw std::runtime_error(
                "InputCouchFile::getLocalDocument openLocalDocument(" +
                std::string(documentName) + ") failed");
    }

    verbose("getLocalDocument(" + std::string(documentName) + ")");
    return {result->json.buf, result->json.size};
}

bool InputCouchFile::isCompletelyNamespaced() const {
    auto value = getSupportsNamespaces();
    return value.has_value() && value.value();
}

bool InputCouchFile::isPartiallyNamespaced() const {
    auto value = getSupportsNamespaces();
    return value.has_value() && !value.value();
}

std::optional<bool> InputCouchFile::getSupportsNamespaces() const {
    auto vbstate = getLocalDocument(LocalDocKey::vbstate);
    nlohmann::json json;
    try {
        json = nlohmann::json::parse(vbstate);
    } catch (const nlohmann::json::exception& e) {
        throw std::invalid_argument(
                "InputCouchFile::getSupportsNamespaces cannot parse "
                " json:" +
                vbstate + " exception:" + e.what());
    }

    std::optional<bool> rv;

    try {
        auto supported = json.at(NamespacesSupportedKey);
        rv = supported.get<bool>();
    } catch (const nlohmann::json::exception&) {
        // no entry - we will return rv uninitialised
    }
    return rv;
}

LocalDocPtr InputCouchFile::openLocalDocument(
        std::string_view documentName) const {
    sized_buf id;
    id.buf = const_cast<char*>(documentName.data());
    id.size = documentName.size();
    LocalDoc* localDoc = nullptr;
    auto errcode =
            couchstore_open_local_document(db, id.buf, id.size, &localDoc);

    switch (errcode) {
    case COUCHSTORE_SUCCESS:
        return LocalDocPtr(localDoc);
    case COUCHSTORE_ERROR_DOC_NOT_FOUND:
        return {};
    default:
        throw std::runtime_error("InputCouchFile::localDocumentExists(" +
                                 std::string(documentName) +
                                 ") error:" + std::to_string(errcode));
    }

    return {};
}
} // end namespace Collections
