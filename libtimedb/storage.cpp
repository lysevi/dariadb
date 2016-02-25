#include "storage.h"
#include "utils.h"

timedb::storage::MemoryStorage::Block::Block(size_t size)
{
    begin = new uint8_t[size];
    end = begin + size;

    memset(begin, 0, size);
	bb = compression::BinaryBuffer(begin, end);
}

timedb::storage::MemoryStorage::Block::~Block()
{
    delete[] begin;
}

timedb::storage::MemoryStorage::MeasChunk::MeasChunk(size_t size, Meas first_m) :
	times(size),
	flags(size),
	values(size),
	first(first_m),
	count(0)
{
	time_compressor = compression::DeltaCompressor(times.bb);
	flag_compressor = compression::FlagCompressor(flags.bb);
	value_compressor = compression::XorCompressor(values.bb);

	
}

timedb::storage::MemoryStorage::MeasChunk::~MeasChunk()
{

}

bool timedb::storage::MemoryStorage::MeasChunk::append(const Meas & m)
{
	auto t_f=time_compressor.append(first.time);
	auto f_f = flag_compressor.append(first.flag);
	auto v_f = value_compressor.append(first.value);

	if (!t_f || !f_f || !v_f) {
		return false;
	}else{
		count++;
		return true;
	}
}

timedb::storage::MemoryStorage::MemoryStorage(size_t size):_size(size){
}


timedb::storage::MemoryStorage::~MemoryStorage()
{
	_chuncks.clear();
}

timedb::Time timedb::storage::MemoryStorage::minTime()
{
    NOT_IMPLEMENTED;
}

timedb::Time timedb::storage::MemoryStorage::maxTime()
{
    NOT_IMPLEMENTED;
}

timedb::append_result timedb::storage::MemoryStorage::append(const timedb::Meas &value)
{
	Chunk_Ptr chunk=nullptr;
	for (auto ch : _chuncks) {
		if ((ch->first.id == value.id) && (!ch->is_full())) {
			chunk = ch;
			break;
		}
	}

	if (chunk == nullptr) {
		chunk = std::make_shared<MeasChunk>(_size, value);
		_chuncks.push_back(chunk);
	}
	else {
		if (!chunk->append(value)) {
			chunk = std::make_shared<MeasChunk>(_size, value);
			_chuncks.push_back(chunk);
		}
	}
	return timedb::append_result(1, 0);
}

timedb::append_result timedb::storage::MemoryStorage::append(const timedb::Meas::PMeas begin, const size_t size)
{
	timedb::append_result result{};
	for (size_t i = 0; i < size; i++) {
		result = result + append(begin[i]);
	}
	return result;
}

timedb::storage::Reader_ptr timedb::storage::MemoryStorage::readInterval(const timedb::IdArray &ids, timedb::Flag flag, timedb::Time from, timedb::Time to)
{
    NOT_IMPLEMENTED;
}

timedb::storage::Reader_ptr timedb::storage::MemoryStorage::readInTimePoint(const timedb::IdArray &ids, timedb::Flag flag, timedb::Time time_point)
{
    NOT_IMPLEMENTED;
}


void timedb::storage::Reader::readAll(Meas::MeasList * output)
{
	while (!isEnd()) {
		readNext(output);
	}
}


timedb::append_result timedb::storage::AbstractStorage::append(const Meas::MeasArray & ma)
{
    timedb::append_result ar{};
    for(auto&m:ma){
        this->append(m);
    }
    return ar;
}

timedb::append_result timedb::storage::AbstractStorage::append(const Meas::MeasList & ml)
{
    timedb::append_result ar{};
    for(auto&m:ml){
        ar=ar+this->append(m);
    }
    return ar;
}

timedb::storage::Reader_ptr timedb::storage::AbstractStorage::readInterval(Time from, Time to)
{
    static timedb::IdArray empty_id{};
    return this->readInterval(empty_id,0,from,to);
}

timedb::storage::Reader_ptr timedb::storage::AbstractStorage::readInTimePoint(Time time_point)
{
    static timedb::IdArray empty_id{};
    return this->readInTimePoint(empty_id,0,time_point);
}



bool timedb::storage::MemoryStorage::InnerReader::isEnd() const
{
    NOT_IMPLEMENTED;
}

void timedb::storage::MemoryStorage::InnerReader::readNext(timedb::Meas::MeasList *output)
{
    NOT_IMPLEMENTED;
}
