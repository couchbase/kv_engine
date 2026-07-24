/*
 *     Copyright 2026-Present Couchbase, Inc.
 *
 *   Use of this software is governed by the Business Source License included
 *   in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
 *   in that file, in accordance with the Business Source License, use of this
 *   software will be governed by the Apache License, Version 2.0, included in
 *   the file licenses/APL2.txt.
 */
#include "openssl_utils.h"

#include <folly/portability/GTest.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509_vfy.h>
#include <memory>
#include <stdexcept>
#include <string>

using unique_evp_pkey_ptr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
using unique_x509_name_ptr =
        std::unique_ptr<X509_NAME, decltype(&X509_NAME_free)>;
using unique_asn1_time_ptr =
        std::unique_ptr<ASN1_TIME, decltype(&ASN1_TIME_free)>;
using unique_x509_store_ptr =
        std::unique_ptr<X509_STORE, decltype(&X509_STORE_free)>;

/// A freshly generated, self-signed CRL, encoded as both PEM and DER, for
/// use as test input to loadCrlFromMemory().
struct TestCrl {
    std::string pem;
    std::string der;
};

/// Build a valid (signed) CRL from scratch so the tests don't depend on
/// any on-disk fixtures.
static TestCrl generateTestCrl() {
    unique_evp_pkey_ptr key(
            EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "prime256v1"),
            &EVP_PKEY_free);
    if (!key) {
        throw std::runtime_error("Failed to generate EC key for test CRL");
    }

    cb::openssl::unique_x509_crl_ptr crl(X509_CRL_new());
    if (!crl) {
        throw std::runtime_error("Failed to allocate X509_CRL");
    }
    X509_CRL_set_version(crl.get(), 1); // CRL v2

    unique_x509_name_ptr name(X509_NAME_new(), &X509_NAME_free);
    X509_NAME_add_entry_by_txt(
            name.get(),
            "CN",
            MBSTRING_ASC,
            reinterpret_cast<const unsigned char*>("Test CRL Issuer"),
            -1,
            -1,
            0);
    X509_CRL_set_issuer_name(crl.get(), name.get());

    unique_asn1_time_ptr lastUpdate(ASN1_TIME_new(), &ASN1_TIME_free);
    X509_gmtime_adj(lastUpdate.get(), 0);
    X509_CRL_set1_lastUpdate(crl.get(), lastUpdate.get());

    unique_asn1_time_ptr nextUpdate(ASN1_TIME_new(), &ASN1_TIME_free);
    X509_gmtime_adj(nextUpdate.get(), 60 * 60 * 24 * 365);
    X509_CRL_set1_nextUpdate(crl.get(), nextUpdate.get());

    if (!X509_CRL_sign(crl.get(), key.get(), EVP_sha256())) {
        throw std::runtime_error("Failed to sign test CRL");
    }

    TestCrl result;

    cb::openssl::unique_bio_ptr pemBio(BIO_new(BIO_s_mem()));
    if (!pemBio || !PEM_write_bio_X509_CRL(pemBio.get(), crl.get())) {
        throw std::runtime_error("Failed to write test CRL as PEM");
    }
    char* pemData = nullptr;
    auto pemLen = BIO_get_mem_data(pemBio.get(), &pemData);
    result.pem.assign(pemData, pemLen);

    unsigned char* derData = nullptr;
    int derLen = i2d_X509_CRL(crl.get(), &derData);
    if (derLen <= 0 || !derData) {
        throw std::runtime_error("Failed to write test CRL as DER");
    }
    result.der.assign(reinterpret_cast<char*>(derData), derLen);
    OPENSSL_free(derData);

    return result;
}

/// Count the entries of a given type (e.g. X509_LU_CRL) currently cached in
/// the store.
static int countObjectsOfType(X509_STORE* store, X509_LOOKUP_TYPE type) {
    int count = 0;
    auto* objects = X509_STORE_get0_objects(store);
    for (int i = 0; i < sk_X509_OBJECT_num(objects); ++i) {
        if (X509_OBJECT_get_type(sk_X509_OBJECT_value(objects, i)) == type) {
            ++count;
        }
    }
    return count;
}

class LoadCrlFromMemoryTest : public ::testing::Test {
protected:
    void SetUp() override {
        crl = generateTestCrl();
    }

    static unique_x509_store_ptr makeStore() {
        return {X509_STORE_new(), &X509_STORE_free};
    }

    TestCrl crl;
};

TEST_F(LoadCrlFromMemoryTest, LoadsSinglePemCrl) {
    auto store = makeStore();
    cb::openssl::loadCrlFromMemory(store.get(), crl.pem);
    EXPECT_EQ(1, countObjectsOfType(store.get(), X509_LU_CRL));
}

TEST_F(LoadCrlFromMemoryTest, LoadsConcatenatedPemCrls) {
    auto store = makeStore();
    auto second = generateTestCrl();
    cb::openssl::loadCrlFromMemory(store.get(), crl.pem + second.pem);
    EXPECT_EQ(2, countObjectsOfType(store.get(), X509_LU_CRL));
}

TEST_F(LoadCrlFromMemoryTest, LoadsSingleDerCrl) {
    auto store = makeStore();
    cb::openssl::loadCrlFromMemory(store.get(), crl.der);
    EXPECT_EQ(1, countObjectsOfType(store.get(), X509_LU_CRL));
}

TEST_F(LoadCrlFromMemoryTest, EmptyDataThrows) {
    auto store = makeStore();
    EXPECT_THROW(cb::openssl::loadCrlFromMemory(store.get(), {}),
                 std::runtime_error);
}

TEST_F(LoadCrlFromMemoryTest, GarbageDataThrows) {
    auto store = makeStore();
    EXPECT_THROW(
            cb::openssl::loadCrlFromMemory(store.get(), "this is not a CRL"),
            std::runtime_error);
    EXPECT_EQ(0, countObjectsOfType(store.get(), X509_LU_CRL));
}
