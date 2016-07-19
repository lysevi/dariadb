#pragma once

#include "../meas.h"
#include "../storage.h"
#include "../utils/fs.h"
#include "bloom_filter.h"
#include <memory>

namespace dariadb {
namespace storage {

const std::string CAP_FILE_EXT = ".cap"; // cola-file extension
const uint32_t CAP_DEFAULT_MAX_LEVELS = 10;

class Capacitor : public MeasStorage {
public:
  struct Params {
    uint32_t B; // measurements count in one datra block
    std::string path;
    size_t max_levels;
    Params(const size_t _B, const std::string _path) {
      B = _B;
      path = _path;
      max_levels = CAP_DEFAULT_MAX_LEVELS;
    }

    size_t measurements_count() const {
      return Params::measurements_count(max_levels, B);
    }

    static uint64_t measurements_count(size_t levels, uint32_t B) {
      uint64_t result = 0;
      for (size_t i = 0; i < levels; ++i) {
        result += B * (uint64_t(1) << i);
      }
      return result + B; //+ memvalues size;
    }
  };
#pragma pack(push, 1)
  struct Header {
    dariadb::Time minTime;
    dariadb::Time maxTime;
    bool is_dropped : 1;
    bool is_closed : 1;         //is correctly closed
    bool is_full : 1;           //is full. normaly is true
    bool is_open_to_write : 1;  //true if oppened to write.
    uint32_t B;                 //one block size. block contains B measurements.
    uint32_t size;              //sizeof file in bytes
    uint32_t _size_B;           //how many block (sizeof(B)) addeded.
    uint8_t  levels_count;      //currently cola levels count
    uint64_t max_values_count;  //maxumim levels
    uint64_t _writed;           //levels addeded
    uint32_t _memvalues_pos;    //values in zero level.
    uint64_t id_bloom;          //bloom filters.
    uint64_t flag_bloom;
    uint64_t transaction_number; // when drop to downlevel storage is non zero.

    bool check_id(const dariadb::Id id) const { return bloom_check(id_bloom, id); }

    bool check_id(const dariadb::IdArray &ids) const {
      for (auto id : ids) {
        if (check_id(id)) {
          return true;
        }
      }
      return false;
    }

    bool check_flag(const dariadb::Flag flag) const {
      if (flag == 0) {
        return true;
      } else {
        return bloom_check(flag_bloom, flag);
      }
    }
  };
#pragma pack(pop)
  virtual ~Capacitor();
  Capacitor(const Capacitor::Params &params, const std::string &fname,
            bool readonly = false);
  static Header readHeader(std::string file_name);
  Header *header();
  append_result append(const Meas &value) override;
  append_result append(const Meas::MeasArray::const_iterator &begin,
                       const Meas::MeasArray::const_iterator &end) override;
  void foreach (const QueryInterval &q, ReaderClb * clbk) override;

  Meas::MeasList readInterval(const QueryInterval &q) override;
  Meas::Id2Meas readInTimePoint(const QueryTimePoint &q) override;
  Meas::Id2Meas currentValue(const IdArray &ids, const Flag &flag) override;

  dariadb::Time minTime() override;
  dariadb::Time maxTime() override;
  bool minMaxTime(dariadb::Id id, dariadb::Time *minResult,
                  dariadb::Time *maxResult) override;
  void flush() override; // write all to storage;

  size_t files_count() const;
  size_t levels_count() const;
  size_t size() const;

  void drop_to_stor(MeasWriter *stor);

  static std::string file_name() { return utils::fs::random_file_name(CAP_FILE_EXT); }
  void fsck();
  void close();

protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

typedef std::shared_ptr<Capacitor> Capacitor_Ptr;
}
}
