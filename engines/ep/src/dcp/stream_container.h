/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2018 Couchbase, Inc
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

/**
 * StreamContainer is a custom container designed for use by the DcpProducer so
 * that it can enable the multiple streams per VB feature. The class is
 * templated primarily to simplify testing and is not attempting to be a
 * generic container.
 *
 * A StreamContainer default constructs to store a single element, as that is
 * how the DcpProducer will initialise a vbucket.
 *
 * A StreamContainer exposes a thread-safe API and internally uses shared
 * locking to enable multiple readers. A read API is exposed via a
 * ReadLockedHandle obtained by calling lock() and a  write API is exposed via
 * WriteLockedHandle obtained by calling wlock().
 *
 * The ReadLockedHandle and WriteLockedHandle both also expose iteration methods
 * begin()/end()/next() so that the elements of the container can be iterated.
 *
 * To support the DcpProducer a ResumableIterationHandle can be obtained by
 * calling startResumable(). A ResumableIterationHandle is an object which
 * provides an iteration style interface, but crucially remembers the current
 * position, this allows the user to begin iterating the elements and destruct
 * their ResumableIterationHandle and later resume from where they had
 * previously iterated to (providing that the StreamContainer membership
 * hasn't changed).
 *
 * Examples of ResumableIterationHandle
 *
 * If the container store integers 5, 4, 3, 2 and 1 an initial loop (begin to
 * end) yields:
 *
 *    54321
 *
 * A new ResumableIterationHandle will from 5, but assume this time we break the
 * loop 3 is returned:
 *
 *    543 <break>
 *
 * The next ResumableIterationHandle will start from 2 and will end when a
 * complete cycle of the elements occurred:
 *
 * Now a new loop will start from 2 and would return true from complete when a
 * full cycle has completed, e.g.
 *
 *    21543
 *
 * Resuming iteration this time again starts from 2:
 *
 *    21543
 *
 * Note1: only a single resume point is maintained, so if two threads were
 * creating ResumableIterationHandle from the same StreamContainer, they will be
 * interfering with each others resume point.
 *
 * Note2: Inserting elements (push_back) or erase() them from the
 * StreamContainer resets the resume point.
 */

#pragma once

#include <forward_list>
#include <shared_mutex>

#include <folly/SharedMutex.h>

template <class Element>
class StreamContainer {
public:
    using container = std::forward_list<Element>;
    using container_itr = typename container::iterator;
    using container_const_itr = typename container::const_iterator;
    using element = Element;

    /**
     * Default construction is deleted, only a StreamContainer with one element
     * should be constructed.
     */
    StreamContainer() = delete;

    /**
     * Create a new StreamContainer with one element
     */
    explicit StreamContainer(Element e) : c{e}, resumePosition{c.begin()} {
    }

    StreamContainer(const StreamContainer&) = delete;
    StreamContainer& operator=(const StreamContainer&) = delete;

    StreamContainer(StreamContainer&&) = default;
    StreamContainer& operator=(StreamContainer&&) = default;

    /**
     * StreamContainer::ResumableIterationHandle
     * This object enables the caller to iterate the StreamContainer, destroy
     * the iterator and later resume iteration from the previous element
     */
    class ResumableIterationHandle {
    public:
        explicit ResumableIterationHandle(StreamContainer& c)
            : sharedLock(c.rwlock),
              container(c),
              startPosition(container.resumePosition),
              currentPosition(startPosition) {
            // call next to prepare the resume position
            container.next(sharedLock);
        }

        /**
         * @return true if we have completed a full cycle of the StreamContainer
         *         elements
         */
        bool complete() {
            // empty or we've reached the start
            bool completed = container.c.empty() ||
                             (cycled && (currentPosition == startPosition));
            if (completed) {
                // Update myqueue with the resume position
                container.resumePosition = currentPosition;
            }
            return completed;
        }

        /**
         * Advance the iterator to the next position in the StreamContainer
         * and handle wrapping around from the end of the underlying container
         */
        void next() {
            currentPosition++;
            if (currentPosition == container.c.end()) {
                cycled = true;
                currentPosition = container.c.begin();
            }
            // Update the resume position owned by the queue
            container.next(sharedLock);
        }

        /// @return a const reference to the current Element
        const Element& get() const {
            return *currentPosition;
        }

    private:
        bool cycled = false;
        std::shared_lock<folly::SharedMutex> sharedLock;
        StreamContainer& container;
        typename container::iterator startPosition;
        typename container::iterator currentPosition;
    };

    /// Internal parent class for the iterable read/write handles
    template <class Iterator>
    class Iterable {
    public:
        bool end() const {
            return itr == endItr;
        }

        const Element& get() const {
            return *itr;
        }

        void next() {
            itr++;
            before++;
        }

    protected:
        void setIterator(Iterator i) {
            itr = i;
        }

        void setBeforeIterator(Iterator i) {
            before = i;
        }

        void setEnd(Iterator i) {
            endItr = i;
        }

        /// The iterator we will increment and access through
        Iterator itr;
        /// The end iterator for implementing end()
        Iterator endItr;
        /// As we use forward_list we need a special 'before' iterator to allow
        /// erase_after
        Iterator before;
    };

    /**
     * StreamContainer::ReadLockedHandle
     * This object obtains read access to the StreamContainer and exposes some
     * methods for inspecting the StreamContainer.
     */
    class ReadLockedHandle : public Iterable<container_const_itr> {
    public:
        explicit ReadLockedHandle(const StreamContainer& c)
            : readLock(c.rwlock), container(c) {
            // sets a const iterator
            this->setIterator(container.c.begin());
            this->setBeforeIterator(container.c.before_begin());
            this->setEnd(container.c.end());
        }

        auto size() const {
            return container.size;
        }

    private:
        std::shared_lock<folly::SharedMutex> readLock;
        const StreamContainer& container;
    };

    /**
     * StreamContainer::WriteLockedHandle
     * This object obtains write access to the StreamContainer and exposes some
     * methods for inspecting and mutating the StreamContainer.
     */
    class WriteLockedHandle : public Iterable<container_itr> {
    public:
        explicit WriteLockedHandle(StreamContainer& c)
            : writeLock(c.rwlock), container(c) {
            this->setIterator(container.c.begin());
            this->setBeforeIterator(container.c.before_begin());
            this->setEnd(container.c.end());
        }

        void swap(Element& e) {
            std::swap((*this->itr), e);
        }

        void push_front(const Element& e) {
            container.push_front(e, writeLock);
        }

        void erase() {
            container.erase_after(this->before, writeLock);
        }

        void clear() {
            container.clear(writeLock);
        }

        bool empty() const {
            return container.c.empty();
        }

    private:
        std::unique_lock<folly::SharedMutex> writeLock;
        StreamContainer& container;
    };

    ResumableIterationHandle startResumable() {
        return ResumableIterationHandle{*this};
    }

    ReadLockedHandle rlock() const {
        return ReadLockedHandle{*this};
    }

    WriteLockedHandle wlock() {
        return WriteLockedHandle{*this};
    }

private:
    /**
     * Push an element to the front of the list and reset the resume iterator
     */
    void push_front(const Element& e, std::unique_lock<folly::SharedMutex>&) {
        c.push_front(e);
        size++;
        resumePosition = c.begin();
    }

    void erase_after(const typename container::iterator& before,
                     std::unique_lock<folly::SharedMutex>&) {
        c.erase_after(before);
        size--;
        resumePosition = c.begin();
    }

    void clear(std::unique_lock<folly::SharedMutex>&) {
        c.clear();
        size = 0;
        resumePosition = c.begin();
    }

    void next(std::shared_lock<folly::SharedMutex>&) {
        if (c.empty()) {
            return;
        }

        resumePosition++;
        if (resumePosition == c.end()) {
            resumePosition = c.begin();
        }
    }

    container c;
    size_t size{1};
    // StreamContainer supports 'resumable' iteration (only one resume point)
    // this object stores where to resume from
    typename container::iterator resumePosition{c.end()};
    mutable folly::SharedMutex rwlock;
};
