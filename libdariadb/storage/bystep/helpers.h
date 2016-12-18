#pragma once
#include <libdariadb/storage/bystep/step_kind.h>
#include <libdariadb/timeutil.h>

namespace dariadb {
namespace storage {
namespace bystep {
size_t step_to_size(STEP_KIND kind);

/// result - rounded time and step in miliseconds
std::tuple<Time, Time> roundTime(const STEP_KIND stepkind, const Time t);

uint64_t intervalForTime(const STEP_KIND stepkind, const size_t valsInInterval,
                         const Time t);
}
}
}