/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2015 Couchbase, Inc.
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
#pragma once

#include <libgreenstack/Request.h>
#include <libgreenstack/Response.h>
#include <libgreenstack/Status.h>
#include <libgreenstack/memcached/Document.h>
#include <string>
#include <cstdint>
#include <memory>

namespace Greenstack {
    class Message;

    class GetRequest : public Greenstack::Request {
    public:
        GetRequest(const std::string& id);

        const std::string getId() const;

    protected:
        GetRequest();

        friend class Message;

        virtual void validate() override;
    };

    class GetResponse : public Greenstack::Response {
    public:
        GetResponse();

        GetResponse(const Greenstack::Status& status);

        std::shared_ptr<DocumentInfo> const& getDocumentInfo() const {
            return documentInfo;
        }

        void setDocumentInfo(
            std::shared_ptr<DocumentInfo> const& documentInfo) {
            GetResponse::documentInfo = documentInfo;
        }

        std::shared_ptr<Buffer> const& getValue() const {
            return value;
        }

        void setValue(std::shared_ptr<Buffer> const& value) {
            GetResponse::value = value;
        }

        void assemble();

        void disassemble();

    protected:
        std::shared_ptr<DocumentInfo> documentInfo;
        std::shared_ptr<Buffer> value;

        friend class Message;

        virtual void validate() override;
    };
}
