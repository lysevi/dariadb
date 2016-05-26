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

class TP_Reader : public storage::Reader {
public:
	TP_Reader();
	~TP_Reader();

	bool isEnd() const;

	dariadb::IdArray getIds() const;

	void readNext(dariadb::storage::ReaderClb *clb) override;

	Reader_ptr clone() const override;

	void reset() override;



	dariadb::Meas::MeasList _values;
	dariadb::Meas::MeasList::iterator _values_iterator;
	dariadb::IdArray _ids;
};

}
}
