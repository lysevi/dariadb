#pragma once

#include "../storage.h"
#include "../utils/locker.h"
#include "chunk.h"

#include <memory>

namespace dariadb {
namespace storage {

typedef std::map<Id, dariadb::storage::ChunksList> ReadChunkMap;

class InnerReader : public Reader {
public:
  InnerReader(dariadb::Flag flag, dariadb::Time from, dariadb::Time to);

  bool isEnd() const override;

  dariadb::IdArray getIds() const override;
  void readNext(storage::ReaderClb *clb) override;

  bool check_meas(const Meas &m) const;

  Reader_ptr clone() const override;
  void reset() override;
  size_t size() override;
  Meas::MeasList _values;
  dariadb::Flag _flag;
  dariadb::Time _from;
  dariadb::Time _to;
  dariadb::IdArray _ids;
  bool end;

  std::mutex _locker, _locker_tp;
};

class TP_Reader : public storage::Reader {
public:
  TP_Reader();
  ~TP_Reader();

  bool isEnd() const override;

  dariadb::IdArray getIds() const override;

  void readNext(dariadb::storage::ReaderClb *clb) override;

  Reader_ptr clone() const override;

  void reset() override;
  size_t size() override;
  dariadb::Meas::MeasList _values;
  dariadb::Meas::MeasList::iterator _values_iterator;
  dariadb::IdArray _ids;
};
}
}
