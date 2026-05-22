# HdrHistogram::to_json() Output Format Documentation

## Overview

The `HdrHistogram::to_json()` method in Couchbase KV-Engine serializes histogram data to JSON format. This format is used for reporting and analyzing latency/performance metrics collected by the server.

## Method Signature

```cpp
nlohmann::json HdrHistogram::to_json() const;
```

## Output Format

The `to_json()` method returns a JSON object with the following structure:

```json
{
  "total": <integer>,
  "bucketsLow": <integer>,
  "data": [
    [<upper_bound>, <count>, <percentile>],
    [<upper_bound>, <count>, <percentile>],
    ...
  ],
  "overflowed": <integer>,
  "overflowed_sum": <integer>,
  "max_trackable": <integer>
}
```

## Field Definitions

### Root-Level Fields

| Field | Type | Description |
|-------|------|-------------|
| `total` | integer | Total count of all non-overflowed samples recorded in the histogram |
| `bucketsLow` | integer | Starting value of the first bucket (lower bound of the first bucket) |
| `data` | array | Array of histogram buckets with [upper_bound, count, percentile] tuples |
| `overflowed` | integer | Number of samples that exceeded `max_trackable` (added in later versions) |
| `overflowed_sum` | integer | Sum of all overflowed sample values (added in later versions) |
| `max_trackable` | integer | Maximum value the histogram can accurately track; values exceeding this are counted in `overflowed` |

### Data Array Format

Each element in the `data` array is a tuple `[upper_bound, count, percentile]`:

| Index | Field | Type | Description |
|-------|-------|------|-------------|
| 0 | `upper_bound` | integer | The maximum value (inclusive) that falls into this bucket |
| 1 | `count` | integer | Number of observations recorded in this bucket |
| 2 | `percentile` | float | Cumulative percentile of observations up to and including this bucket (0-100) |

### Example

```json
{
  "total": 1000,
  "bucketsLow": 50000,
  "data": [
    [100000, 150, 15.0],
    [200000, 200, 35.0],
    [400000, 350, 70.0],
    [800000, 300, 100.0]
  ],
  "overflowed": 5,
  "overflowed_sum": 50000000,
  "max_trackable": 3600000000000
}
```

In this example:
- 1000 total samples were recorded
- The first bucket represents values from 50000 to 100000, with 150 observations (15% of total)
- The second bucket represents values from 100000 to 200000, with 200 observations (35% cumulative)
- The third bucket represents values from 200000 to 400000, with 350 observations (70% cumulative)
- The fourth bucket represents values from 400000 to 800000, with 300 observations (100% cumulative)
- 5 samples exceeded the maximum trackable value of 3,600,000,000,000

## Bucket Iteration Strategy

The `data` array is generated using a **percentile iterator** with 5 iteration steps per half-distance to 100%. This means:

1. The histogram uses logarithmic bucket sizing internally (HDR Histogram algorithm)
2. Only buckets with recorded observations are included in the output
3. Percentiles are cumulative (each entry shows % of all samples with values ≤ upper_bound)
4. The percentile calculation stops at the last non-overflowed bucket (100% does not include overflowed samples)

## Overflow Handling

- **Samples exceeding `max_trackable`**: Counted separately in the `overflowed` field
- **Overflow Sum**: The `overflowed_sum` field tracks the sum of all overflowed values, useful for calculating accurate statistics including overflowed samples
- **Why separate?**: The underlying C HDR histogram implementation doesn't track overflowed values in its buckets, so they are tracked separately and added explicitly to the JSON output

## Use Cases

This JSON format is used by:

1. **cbstats**: Timings and performance metric reporting
2. **Prometheus**: Histogram metric export and monitoring
3. **Diagnostics Tools**: Server performance analysis tools (e.g., `mctimings`)
4. **Client Libraries**: Performance monitoring and latency tracking

## Conversion to HistogramData

The KV-Engine uses a `HistogramData` struct to normalize histogram representations. When an `HdrHistogram` is converted to `HistogramData`:

- Each bucket from the JSON becomes a `HistogramBucket` with `{lowerBound, upperBound, count}`
- The `mean` is calculated from the histogram data
- The `sampleCount` includes both regular and overflowed samples
- The `sampleSum` is approximated from bucket midpoints × bucket counts

## Implementation Details

The `to_json()` method:
- Is thread-safe (uses RWLock internally via `folly::Synchronized`)
- Performs a percentile iteration through the histogram
- Automatically includes overflow information (if any samples overflowed)
- Returns an `nlohmann::json` object compatible with the JSON for Modern C++ library

### Key Implementation (from `platform/hdrhistogram/hdrhistogram.cc`):

```cpp
nlohmann::json HdrHistogram::to_json() const {
    nlohmann::json rootObj;
    auto itr = makePercentileIterator(5);  // 5 steps per half-distance
    
    rootObj["total"] = itr.total_count;
    rootObj["bucketsLow"] = itr.value_iterated_from;
    nlohmann::json dataArr;
    
    while (itr != end()) {
        dataArr.push_back({itr->upper_bound, itr->count, *itr->percentile});
        ++itr;
    }
    rootObj["data"] = dataArr;
    rootObj["overflowed"] = overflowed.load();
    rootObj["overflowed_sum"] = overflowedSum.load();
    rootObj["max_trackable"] = this->getMaxTrackableValue();
    
    return rootObj;
}
```

## Serialization to String

To get a string representation of the histogram data:

```cpp
std::string str = histogram.to_string();  // Returns JSON as string
```

This calls `to_json().dump()` internally.

## Related Types

- **`Bucket` struct**: Represents a single histogram bucket with `{lower_bound, upper_bound, count, percentile}`
- **`HistogramData`**: Common normalized format used by stat collectors
- **Iterator types**: `Iterator`, `LinearIterator`, `LogarithmicIterator`, `PercentileIterator` for traversing histogram buckets

## Version Notes

- **Overflow fields** (`overflowed`, `overflowed_sum`, `max_trackable`): Added in later versions of Couchbase to support better overflow handling and diagnostic information
- The JSON format is backwards compatible; consumers may treat overflow fields as optional (defaulting to 0 if absent)

## References

- Source: `platform/include/hdrhistogram/hdrhistogram.h` (method declaration at line 458)
- Source: `platform/hdrhistogram/hdrhistogram.cc` (implementation starting at line 384)
- Underlying C library: `HdrHistogram_c` (used by Couchbase for high-performance histogram collection)
- Consumer: `utilities/timing_histogram_printer.h/cc` (example consumer that parses and displays this JSON format)
