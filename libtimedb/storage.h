#pragma once

#include "compression.h"
#include <list>
#include <memory>
namespace timedb{
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
				compression::BinaryBuffer bb;
				Block(size_t size);
				~Block();
				bool is_full()const { return bb.free_size() == 0; }
			};

            struct MeasChunk
            {
                Block times;
                Block flags;
                Block values;
				compression::DeltaCompressor time_compressor;
				compression::FlagCompressor  flag_compressor;
				compression::XorCompressor   value_compressor;
				size_t count;
				Meas first;
                MeasChunk(size_t size, Meas first_m);
                ~MeasChunk();
				bool append(const Meas&m);
				bool is_full()const { return times.is_full() || flags.is_full() || values.is_full(); }
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
				InnerReader(timedb::Flag flag, timedb::Time from, timedb::Time to);
				void add(Chunk_Ptr c, size_t count);
                bool isEnd() const override;
                void readNext(Meas::MeasList*output)  override;
			protected:
				bool check_meas(Meas&m);
			protected:
				std::list<ReadChunk> _chunks;
				timedb::Flag _flag;
				timedb::Time _from;
				timedb::Time _to;
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
		protected:
			size_t _size;

			ChuncksList _chuncks;
			Time _min_time,_max_time;
		};
	}
}
