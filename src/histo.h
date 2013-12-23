/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2010 Couchbase, Inc
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

#ifndef SRC_HISTO_H_
#define SRC_HISTO_H_ 1

#include "config.h"

#include <algorithm>
#include <cmath>
#include <functional>
#include <iterator>
#include <limits>
#include <numeric>
#include <ostream>
#include <vector>

#include "atomic.h"
#include "common.h"

// Forward declaration.
template <typename T>
class Histogram;

/**
 * An individual bin of histogram data.
 */
template <typename T>
class HistogramBin {
public:

    /**
     * Get a HistogramBin covering [s, e)
     */
    HistogramBin(T s, T e) : _count(0), _start(s), _end(e) {}

    /**
     * The starting value of this histogram bin (inclusive).
     */
    T start() const { return _start; }

    /**
     * The ending value of this histogram bin (exclusive).
     */
    T end() const { return _end; }

    /**
     * The count in this bin.
     */
    size_t count() const { return _count.load(); }

private:
    friend class Histogram<T>;

    /**
     * Increment this bin by the given amount.
     */
    void incr(size_t amount) {
        _count.fetch_add(amount);
    }

    /**
     * Set a specific value for this bin.
     */
    void set(size_t val) {
        _count.store(val);
    }

    /**
     * Does this bin contain the given value?
     *
     * @param value a value that may be within this bin's boundaries
     * @return true if this value is counted within this bin
     */
    bool accepts(T value) {
        return value >= _start &&
            (value < _end || value == std::numeric_limits<T>::max());
    }

    AtomicValue<size_t> _count;
    T              _start;
    T              _end;

    DISALLOW_COPY_AND_ASSIGN(HistogramBin);
};

/**
 * Helper function object to sum sample.
 */
template <typename T>
class HistogramBinSampleAdder {
public:
    HistogramBinSampleAdder() {}
    size_t operator() (size_t n, const HistogramBin<T> *b) { return n + b->count(); }
};

/**
 * A bin generator that generates buckets of a width that may increase
 * in size a fixed growth amount over iterations.
 */
template <typename T>
class GrowingWidthGenerator {
public:

    /**
     * Construct a growing width generator.
     *
     * @param start the starting point for this generator (inclusive)
     * @param width the starting width for this generator
     * @param growth how far the width should increase each time
     */
    GrowingWidthGenerator(T start, T width, double growth=1.0)
        : _growth(growth), _start(start), _width(static_cast<double>(width)) {}

    /**
     * Generate the next bin.
     */
    HistogramBin<T>* operator () () {
        HistogramBin<T>* rv = new HistogramBin<T>(_start,
                                                  _start + static_cast<T>(_width));
        _start += static_cast<T>(_width);
        _width = _width * _growth;
        return rv;
    }

private:
    double _growth;
    T      _start;
    double _width;
};

/**
 * A bin generator that generates buckets from a sequence of T where
 * each bin is from [v[n], v[n+1]).
 */
template <typename T>
class FixedInputGenerator {
public:

    /**
     * Get a FixedInputGenerator with the given sequence of bin starts.
     */
    FixedInputGenerator(std::vector<T> &input)
        : it(input.begin()), end(input.end()) {}

    HistogramBin<T>* operator () () {
        assert(it != end);
        T current = *it;
        ++it;
        assert(it != end);
        T next = *it;
        return new HistogramBin<T>(current, next);
    }
private:
    typename std::vector<T>::iterator it;
    typename std::vector<T>::iterator end;
};

/**
 * A bin generator that [n^i, n^(i+1)).
 */
template <typename T>
class ExponentialGenerator {
public:

    /**
     * Get a FixedInputGenerator with the given sequence of bin starts.
     */
    ExponentialGenerator(uint64_t start, double power)
        : _start(start), _power(power) {}

    HistogramBin<T>* operator () () {
        T start = static_cast<T>(std::pow(_power, static_cast<double>(_start)));
        T end = static_cast<T>(std::pow(_power, static_cast<double>(++_start)));
        return new HistogramBin<T>(start, end);
    }
private:
    uint64_t _start;
    double   _power;
};

/**
 * Comparator for finding a histogram bin to hold a value.
 */
template <typename T>
class BinCompare {
public:

    BinCompare() {}

    bool operator() (T t, HistogramBin<T> *b) {
        return t < b->end();
    }
};

/**
 * A Histogram.
 */
template <typename T>
class Histogram {
public:

    /**
     * Build a histogram.
     *
     * @param generator a generator for the bins within this bucket
     * @param n how many bins this histogram should contain
     */
    template <typename G>
    Histogram(const G &generator, size_t n=30) : bins(n) {
        fill(generator);
    }

    /**
     * Build a default histogram.
     *
     * @param n how many bins this histogram should contain.
     */
    Histogram(size_t n=30) : bins(n) {
        ExponentialGenerator<T> generator(0, 2.0);
        fill(generator);
    }

    ~Histogram() {
        for (typename std::vector<HistogramBin<T>*>::iterator it = bins.begin();
             it != bins.end(); ++it) {
            delete *it;
        }
    }

    /**
     * Add a value to this histogram.
     *
     * @param amount the size of the thing being added
     * @param count the quantity at this size being added
     */
    void add(T amount, size_t count=1) {
        findBin(amount)->incr(count);
    }

    /**
     * Get the bin servicing the given sized input.
     */
    const HistogramBin<T>* getBin(T amount) {
        return findBin(amount);
    }

    /**
     * Set all bins to 0.
     */
    void reset() {
        std::for_each(bins.begin(), bins.end(),
                      std::bind2nd(std::mem_fun(&HistogramBin<T>::set), 0));
    }

    /**
     * Get the total number of samples counted.
     *
     * This is the sum of all counts in each bin.
     */
    size_t total() {
        HistogramBinSampleAdder<T> a;
        return std::accumulate(begin(), end(), 0, a);
    }

    /**
     * A HistogramBin iterator.
     */
    class iterator  : public std::iterator<std::forward_iterator_tag,
                                           const Histogram<T>*> {
    public:
        iterator(typename std::vector<HistogramBin<T>*>::iterator x) :p(x) {}
        iterator(const iterator& mit) : p(mit.p) {}
        iterator& operator++() {++p;return *this;}
        iterator& operator++(int) {iterator tmp(*this); operator++(); return tmp;}
        bool operator==(const iterator& rhs) {return p==rhs.p;}
        bool operator!=(const iterator& rhs) {return p!=rhs.p;}
        const HistogramBin<T>* operator*() {return *p;}
    private:
        typename std::vector<HistogramBin<T>*>::iterator p;
        friend class Histogram<T>;
    };

    /**
     * Get an iterator from the beginning of a histogram bin.
     */
    iterator begin() const {
        return iterator(const_cast<Histogram<T>*>(this)->bins.begin());
    }

    /**
     * Get the iterator at the end of the histogram bin.
     */
    iterator end() const {
        return iterator(const_cast<Histogram<T>*>(this)->bins.end());
    }

private:

    template <typename G>
    void fill(G &generator) {
        std::generate(bins.begin(), bins.end(), generator);

        // If there will not naturally be one, create a bin for the
        // smallest possible value
        if (bins.front()->start() > std::numeric_limits<T>::min()) {
            bins.insert(bins.begin(),
                        new HistogramBin<T>(std::numeric_limits<T>::min(),
                                            bins.front()->start()));
        }

        // Also create one reaching to the largest possible value
        if (bins.back()->end() < std::numeric_limits<T>::max()) {
            bins.push_back(new HistogramBin<T>(bins.back()->end(),
                                               std::numeric_limits<T>::max()));
        }

        verify();
    }

    // This validates that we're sorted and have no gaps or overlaps.
    void verify() {
        T prev = std::numeric_limits<T>::min();
        typename std::vector<HistogramBin<T>*>::iterator it;
        int pos(0);
        for (it = bins.begin(); it != bins.end(); ++it) {
            if ((*it)->start() != prev) {
                std::cerr << "Expected " << (*it)->start() << " == " << prev
                          << " at pos " << pos << std::endl;
                abort();
            }
            assert((*it)->start() == prev);
            prev = (*it)->end();
            ++pos;
        }
        assert(prev == std::numeric_limits<T>::max());
    }

    HistogramBin<T> *findBin(T amount) {
        HistogramBin<T> *rv(NULL);
        if (amount == std::numeric_limits<T>::max()) {
            rv = bins.back();
        } else {
            typename std::vector<HistogramBin<T>*>::iterator it;
            BinCompare<T> binCompare;
            it = std::upper_bound(bins.begin(), bins.end(), amount, binCompare);
            assert(it != bins.end());
            assert((*it)->accepts(amount));
            rv = *it;
        }
        return rv;
    }

    template <typename Ttype>
    friend std::ostream& operator<< (std::ostream& out,
                                     const Histogram<Ttype> &b);

    std::vector<HistogramBin<T>*> bins;

    DISALLOW_COPY_AND_ASSIGN(Histogram);
};

/**
 * Times blocks automatically and records the values in a histogram.
 */
class BlockTimer {
public:

    /**
     * Get a BlockTimer that will store its values in the given
     * histogram.
     *
     * @param d the histogram that will hold the result
     */
    BlockTimer(Histogram<hrtime_t> *d, const char *n=NULL, std::ostream *o=NULL)
        : dest(d), start(gethrtime()), name(n), out(o) {}

    ~BlockTimer() {
        hrtime_t spent(gethrtime() - start);
        dest->add(spent / 1000);
        log(spent, name, out);
    }

    static inline void log(hrtime_t spent, const char *name, std::ostream *o) {
        if (o && name) {
            *o << name << "\t" << spent << "\n";
        }
    }

private:
    Histogram<hrtime_t> *dest;
    hrtime_t             start;
    const char          *name;
    std::ostream        *out;
};

// How to print a bin.
template <typename T>
std::ostream& operator <<(std::ostream &out, const HistogramBin<T> &b) {
    out << "[" << b.start() << ", " << b.end() << ") = " << b.count();
    return out;
}

// How to print a vector histogram bin pointers
template <typename T>
std::ostream& operator <<(std::ostream &out, const std::vector<HistogramBin<T>*> &b) {
    bool needComma(false);
    for (typename std::vector<HistogramBin<T>*>::const_iterator it = b.begin();
         it != b.end(); ++it) {
        if (needComma) {
            out << ", ";
        }
        out << **it;
        needComma = true;
    }
    return out;
}

// How to print a histogram.
template <typename T>
std::ostream& operator <<(std::ostream &out, const Histogram<T> &b) {
    out << "{Histogram: " << b.bins << "}";
    return out;
}

#endif  // SRC_HISTO_H_
