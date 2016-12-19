#pragma once

#include <libdariadb/interfaces/imeasstorage.h>
#include <libdariadb/storage/chunk.h>
#include <libdariadb/st_exports.h>

#include <list>

namespace dariadb {
namespace storage {

using ChunksList = std::list<Chunk_Ptr>;
class IOAdapter {
public:
	struct Description
	{
		size_t in_queue;
	};
  EXPORT IOAdapter(const std::string &fname);
  EXPORT ~IOAdapter();
  EXPORT Description description();
  EXPORT void stop();
  /// min/max - real min/max values. chunk can be not filled.
  EXPORT void append(const Chunk_Ptr&ch, Time min, Time max);
  EXPORT ChunksList readInterval(uint64_t period_from, uint64_t period_to, Id meas_id);
  EXPORT Chunk_Ptr readTimePoint(uint64_t period, Id meas_id);
  EXPORT void replace(const Chunk_Ptr&ch, Time min, Time max);
  EXPORT Time minTime();
  EXPORT Time maxTime();
  EXPORT bool minMaxTime(Id id, Time *minResult, Time *maxResult);
  EXPORT void flush();
private:
  class Private;
  std::unique_ptr<Private> _impl;
};
}
}