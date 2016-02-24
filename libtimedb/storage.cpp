#include "storage.h"
#include "utils.h"

timedb::storage::MemoryStorage::Block::Block(size_t size)
{
	time_begin = new uint8_t[size];
	time_end = time_begin + size;

	value_begin = new uint8_t[size];
	value_end = value_begin + size;

	flag_begin = new uint8_t[size];
	flag_end = flag_begin + size;

	memset(time_begin, 0, size);
	memset(flag_begin, 0, size);
	memset(value_begin, 0, size);
}

timedb::storage::MemoryStorage::Block::~Block()
{
	delete[] time_begin;
	delete[] value_begin;
	delete[] flag_begin;
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
