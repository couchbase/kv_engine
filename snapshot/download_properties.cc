/*
 *     Copyright 2024-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */

#include "download_properties.h"
#include <nlohmann/json.hpp>
#include <platform/base64.h>

namespace cb::snapshot {
void to_json(nlohmann::json& json, const DownloadProperties& prop) {
    json = {{"host", prop.hostname},
            {"port", prop.port},
            {"bucket", prop.bucket}};

    if (prop.fsync_interval.has_value()) {
        json["fsync_interval"] = *prop.fsync_interval;
    }
    if (prop.write_size.has_value()) {
        json["write_size"] = *prop.write_size;
    }
    if (prop.allow_fail_fast.has_value()) {
        json["allow_fail_fast"] = *prop.allow_fail_fast;
    }
    if (prop.sasl.has_value()) {
        json["sasl"] = *prop.sasl;
    }
    if (prop.tls.has_value()) {
        json["tls"] = *prop.tls;
    }
}

void to_json(nlohmann::json& json, const DownloadProperties::Sasl& sasl) {
    json = {{"mechanism", sasl.mechanism},
            {"username", sasl.username},
            {"password", sasl.password}};
}

void to_json(nlohmann::json& json, const DownloadProperties::Tls& tls) {
    json = {{"cert", tls.cert},
            {"key", tls.key},
            {"ca_store", tls.ca_store},
            {"ssl_peer_verify", tls.ssl_peer_verify},
            {"passphrase", cb::base64::encode(tls.passphrase)}};
}

void from_json(const nlohmann::json& json, DownloadProperties& prop) {
    prop.hostname = json.value("host", "");
    prop.port = json.value("port", 0);
    prop.bucket = json.value("bucket", "");

    if (json.contains("fsync_interval")) {
        prop.fsync_interval = json["fsync_interval"];
    }
    if (json.contains("write_size")) {
        prop.write_size = json["write_size"];
    }
    if (json.contains("allow_fail_fast")) {
        prop.allow_fail_fast = json["allow_fail_fast"];
    }
    if (json.contains("sasl")) {
        prop.sasl = json["sasl"];
    }
    if (json.contains("tls")) {
        prop.tls = json["tls"];
    }
}

void from_json(const nlohmann::json& json, DownloadProperties::Sasl& sasl) {
    sasl.mechanism = json.value("mechanism", "");
    sasl.username = json.value("username", "");
    sasl.password = json.value("password", "");
}

void from_json(const nlohmann::json& json, DownloadProperties::Tls& tls) {
    tls.cert = json.value("cert", "");
    tls.key = json.value("key", "");
    tls.ca_store = json.value("ca_store", "");
    tls.ssl_peer_verify = json.value("ssl_peer_verify", true);
    if (json.contains("passphrase")) {
        tls.passphrase = cb::base64::decode(json.value("passphrase", ""));
    }
}

} // namespace cb::snapshot
