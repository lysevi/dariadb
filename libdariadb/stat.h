#pragma once

#include <libdariadb/meas.h>
#include <libdariadb/storage/bloom_filter.h>

namespace dariadb {
#pragma pack(push, 1)
struct Statistic {
  Time minTime;
  Time maxTime;

  uint32_t count; /// count of stored values.

  uint64_t flag_bloom;

  Value minValue;

  Value maxValue;

  Value sum;

  Statistic() {
    flag_bloom = storage::bloom_empty<Flag>();
    count = uint32_t(0);
    minTime = MAX_TIME;
    maxTime = MIN_TIME;

    minValue = MAX_VALUE;
    maxValue = MIN_VALUE;

    sum = Value(0);
  }

  void update(const Meas &m) {
    count++;

    minTime = std::min(m.time, minTime);
    maxTime = std::max(m.time, maxTime);

    flag_bloom = storage::bloom_add<Flag>(flag_bloom, m.flag);

    minValue = std::min(m.value, minValue);
    maxValue = std::max(m.value, maxValue);

    sum += m.value;
  }

  void update(const Statistic &st) {
    count += st.count;

    minTime = std::min(st.minTime, minTime);
    maxTime = std::max(st.maxTime, maxTime);

    flag_bloom = storage::bloom_combine(flag_bloom, st.flag_bloom);

    minValue = std::min(st.minValue, minValue);
    maxValue = std::max(st.maxValue, maxValue);

    sum += st.sum;
  }
};
#pragma pack(pop)
}