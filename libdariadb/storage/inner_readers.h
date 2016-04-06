#pragma once

#include "../storage.h"
#include "chunk.h"

#include <memory>
#include <mutex>

namespace dariadb {
	namespace storage {

        typedef std::map<Id, dariadb::storage::ChuncksList> ReadChunkMap;

		class InnerReader : public Reader {
		public:

			InnerReader(dariadb::Flag flag, dariadb::Time from, dariadb::Time to);

            void add(Chunk_Ptr c);

            void add_tp(Chunk_Ptr c);
			bool isEnd() const override;

			dariadb::IdArray getIds()const override;
			void readNext(storage::ReaderClb*clb) override;
			void readTimePoint(storage::ReaderClb*clb);


			bool check_meas(const Meas&m)const;

			Reader_ptr clone()const override;
			void reset()override;
		
			bool is_time_point_reader;

			ReadChunkMap _chunks;
			ReadChunkMap _tp_chunks;
			dariadb::Flag _flag;
			dariadb::Time _from;
			dariadb::Time _to;
			bool _tp_readed;
			bool end;
			IdArray _not_exist;

			typedef std::tuple<dariadb::Id, dariadb::Time> IdTime;
			std::set<IdTime> _tp_readed_times;

			std::mutex _mutex, _mutex_tp;


		};

		class InnerCurrentValuesReader : public Reader {
		public:
			InnerCurrentValuesReader();
			~InnerCurrentValuesReader();

			bool isEnd() const override;

			void readCurVals(storage::ReaderClb*clb);

			void readNext(storage::ReaderClb*clb) override;

			IdArray getIds()const;
			Reader_ptr clone()const;
			void reset();

			bool end;
			std::mutex _mutex;
			dariadb::Meas::MeasList _cur_values;
		};

	}
}
