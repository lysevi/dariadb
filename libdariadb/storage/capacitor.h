#pragma once

#include "../interfaces/imeasstorage.h"
#include "../meas.h"
#include "../utils/fs.h"
#include "bloom_filter.h"
#include <memory>

namespace dariadb {
namespace storage {

const std::string CAP_FILE_EXT = ".cap"; // cola-file extension

class Capacitor : public IMeasStorage {
public:
#pragma pack(push, 1)
  struct Header {
    dariadb::Time minTime;
    dariadb::Time maxTime;
    bool is_dropped : 1;
    bool is_closed : 1;        // is correctly closed
    bool is_full : 1;          // is full. normaly is true
    bool is_open_to_write : 1; // true if oppened to write.
    uint32_t B;                // one block size. block contains B measurements.
    uint64_t size;             // sizeof file in bytes
    uint32_t _size_B;          // how many block (sizeof(B)) addeded.
    uint8_t levels_count;      // currently cola levels count
    uint64_t max_values_count; // maxumim levels
    uint64_t _writed;          // levels addeded
    uint32_t _memvalues_pos;   // values in zero level.
    uint64_t id_bloom;         // bloom filters.
    uint64_t flag_bloom;

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
  Capacitor(const std::string &fname,
            bool readonly = false);
  static Header readHeader(std::string file_name);
  Header *header();
  append_result append(const Meas &value) override;
  append_result append(const Meas::MeasArray::const_iterator &begin,
                       const Meas::MeasArray::const_iterator &end) override;
  void foreach (const QueryInterval &q, IReaderClb * clbk) override;
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

  static std::string rnd_file_name() { return utils::fs::random_file_name(CAP_FILE_EXT); }
  std::string file_name()const;
  void fsck();
  void close();

  Meas::MeasArray readAll()const;
protected:
  class Private;
  std::unique_ptr<Private> _Impl;
};

typedef std::shared_ptr<Capacitor> Capacitor_Ptr;
}
}
