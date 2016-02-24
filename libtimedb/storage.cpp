#include "storage.h"
#include "utils.h"

timedb::storage::MemoryStorage::Block::Block(size_t size)
{
    begin = new uint8_t[size];
    end = begin + size;

    memset(begin, 0, size);

}

timedb::storage::MemoryStorage::Block::~Block()
{
    delete[] begin;
}

timedb::storage::MemoryStorage::MeasChunk::MeasChunk(size_t size):
    times(size),
    flags(size),
    values(size)
{

}

timedb::storage::MemoryStorage::MeasChunk::~MeasChunk()
{

}

timedb::storage::MemoryStorage::MemoryStorage(size_t size):_size(size)
{
}


timedb::storage::MemoryStorage::~MemoryStorage()
{
}


void timedb::storage::Reader::readAll(Meas::MeasList * output)
{
	while (!isEnd()) {
		readNext(output);
	}
}


timedb::append_result timedb::storage::AbstractStorage::append(const Meas::MeasArray & ma)
{
	NOT_IMPLEMENTED
}

timedb::append_result timedb::storage::AbstractStorage::append(const Meas::MeasList & ml)
{
	NOT_IMPLEMENTED
}

timedb::storage::Reader_ptr timedb::storage::AbstractStorage::readInterval(Time from, Time to)
{
	NOT_IMPLEMENTED
}

timedb::storage::Reader_ptr timedb::storage::AbstractStorage::readInTimePoint(Time time_point)
{
	NOT_IMPLEMENTED
}


