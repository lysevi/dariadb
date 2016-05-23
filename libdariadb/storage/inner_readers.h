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

  void add(Cursor_ptr c);

  void add_tp(Chunk_Ptr c);
  bool isEnd() const override;

  dariadb::IdArray getIds() const override;
  void readNext(storage::ReaderClb *clb) override;
  void readTimePoint(storage::ReaderClb *clb);

  bool check_meas(const Meas &m) const;

  Reader_ptr clone() const override;
  void reset() override;

  bool is_time_point_reader;

  CursorList _cursors;
  ReadChunkMap _tp_chunks;
  dariadb::Flag _flag;
  dariadb::Time _from;
  dariadb::Time _to;
  dariadb::IdArray _ids;
  bool _tp_readed;
  bool end;
  IdArray _not_exist;

  typedef std::tuple<dariadb::Id, dariadb::Time> IdTime;
  std::set<IdTime> _tp_readed_times;

  std::mutex _locker, _locker_tp;
};

class InnerCurrentValuesReader : public Reader {
public:
  InnerCurrentValuesReader();
  ~InnerCurrentValuesReader();

  bool isEnd() const override;

  void readCurVals(storage::ReaderClb *clb);

  void readNext(storage::ReaderClb *clb) override;

  IdArray getIds() const override;
  Reader_ptr clone() const override;
  void reset() override;

  bool end;
  std::mutex _locker;
  dariadb::Meas::MeasList _cur_values;
};
}
}
