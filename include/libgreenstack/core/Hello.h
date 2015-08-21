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
#include <string>

namespace Greenstack {
    class Message;

    class HelloRequestBuilder {
    public:
        void setUserAgent(const std::string& userAgent) {
            HelloRequestBuilder::userAgent = userAgent;
        }

        void setUserAgentVersion(const std::string& agentVersion) {
            HelloRequestBuilder::userAgentVersion = agentVersion;
        }

        void setComment(const std::string& comment) {
            HelloRequestBuilder::comment = comment;
        }

        Message Finish() {
            Request ret;
            ret.setOpcode(Opcode::Hello);
            setPayload(ret);
            return ret;
        }

    protected:
        std::string userAgent;
        std::string userAgentVersion;
        std::string comment;

        void setPayload(Message& message);
    };

    class HelloRequest : public Greenstack::Request {
    public:
        HelloRequest(const std::string& userAgent,
                     const std::string& agentVersion,
                     const std::string& comment = std::string());

        const std::string getUserAgent() const;

        const std::string getUserAgentVersion() const;

        const std::string getComment() const;

    protected:
        HelloRequest();

        virtual void validate() override;

        friend class Message;
    };

    class HelloResponse : public Greenstack::Response {
    public:
        HelloResponse(const std::string& userAgent,
                      const std::string& agentVersion,
                      const std::string& mechanisms = std::string());

        const std::string getUserAgent() const;

        const std::string getUserAgentVersion() const;

        const std::string getSaslMechanisms() const;

    protected:
        HelloResponse();

        virtual void validate() override;

        friend class Message;
    };
}
