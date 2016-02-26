#pragma once

#include "compression.h"
#include <list>
#include <memory>
#include <mutex>

namespace memseries{
	namespace storage {

		class Reader
		{
		public:
			virtual bool isEnd() const = 0;
			virtual void readNext(Meas::MeasList*output) = 0;
			virtual void readAll(Meas::MeasList*output);
		};

		typedef std::shared_ptr<Reader> Reader_ptr;
		class AbstractStorage {
		public:
			virtual ~AbstractStorage() = default;
			/// min time of writed meas
			virtual Time minTime() = 0;
			/// max time of writed meas
			virtual Time maxTime() = 0;

			virtual append_result append(const Meas& value) = 0;
			virtual append_result append(const Meas::MeasArray& ma);
			virtual append_result append(const Meas::MeasList& ml);
			virtual append_result append(const Meas::PMeas begin, const size_t size) = 0;

			virtual Reader_ptr readInterval(Time from, Time to);
			virtual Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from, Time to) = 0;

			virtual Reader_ptr readInTimePoint(Time time_point);
			virtual Reader_ptr readInTimePoint(const IdArray &ids, Flag flag, Time time_point) = 0;

		};

		class MemoryStorage:public AbstractStorage
		{
			struct Block
			{
                uint8_t *begin;
                uint8_t *end;
				Block(size_t size);
				~Block();
			};

            struct MeasChunk
            {
                Block times;
                Block flags;
                Block values;
				compression::CopmressedWriter c_writer;
				size_t count;
				Meas first;
                MeasChunk(size_t size, Meas first_m);
                ~MeasChunk();
				bool append(const Meas&m);
				bool is_full()const { return c_writer.is_full(); }
            };
			typedef std::shared_ptr<MeasChunk> Chunk_Ptr;
			typedef std::list<Chunk_Ptr> ChuncksList;
        public:
            class InnerReader:public Reader{
            public:
				struct ReadChunk
				{
					size_t    count;
					Chunk_Ptr chunk;
				};
				InnerReader(memseries::Flag flag, memseries::Time from, memseries::Time to);
				void add(Chunk_Ptr c, size_t count);
                bool isEnd() const override;
                void readNext(Meas::MeasList*output)  override;
			protected:
				bool check_meas(Meas&m);
			protected:
				std::list<ReadChunk> _chunks;
				memseries::Flag _flag;
				memseries::Time _from;
				memseries::Time _to;
				ReadChunk _next;
            };
        public:
			MemoryStorage(size_t size);
			virtual ~MemoryStorage();

            using AbstractStorage::append;
            using AbstractStorage::readInterval;
            using AbstractStorage::readInTimePoint;

            Time minTime();
            Time maxTime();
            append_result append(const Meas& value)override;
            append_result append(const Meas::PMeas begin, const size_t size)override;
            Reader_ptr readInterval(const IdArray &ids, Flag flag, Time from, Time to)override;
            Reader_ptr readInTimePoint(const IdArray &ids, Flag flag, Time time_point)override;

			size_t size()const { return _size; }
			size_t chinks_size()const { return _chuncks.size(); }
		protected:
			size_t _size;

			ChuncksList _chuncks;
			Time _min_time,_max_time;

            std::mutex _mutex;
		};
	}
}
