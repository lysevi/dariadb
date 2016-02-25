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

timedb::storage::MemoryStorage::MemoryStorage(size_t size):
	_size(size),
	_min_time(std::numeric_limits<timedb::Time>::max()),
	_max_time(std::numeric_limits<timedb::Time>::min())
{
}


timedb::storage::MemoryStorage::~MemoryStorage()
{
	_chuncks.clear();
}

timedb::Time timedb::storage::MemoryStorage::minTime()
{
	return _min_time;
}

timedb::Time timedb::storage::MemoryStorage::maxTime()
{
	return _max_time;
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
	_min_time = std::min(_min_time, value.time);
	_max_time = std::max(_max_time, value.time);
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
	auto res= std::make_shared<InnerReader>(flag, from, to);
	for (auto ch : _chuncks) {
		if ((ids.size() == 0) || (std::find(ids.begin(), ids.end(), ch->first.id) != ids.end())) {
			if (utils::inInterval(from, to, minTime()) || utils::inInterval(from, to, maxTime())) {
				res->add(ch, ch->count);
			}
		}
	}
	
	return res;
}

timedb::storage::Reader_ptr timedb::storage::MemoryStorage::readInTimePoint(const timedb::IdArray &ids, timedb::Flag flag, timedb::Time time_point)
{
	return this->readInterval(ids, flag, 0, time_point);
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



timedb::storage::MemoryStorage::InnerReader::InnerReader(timedb::Flag flag, timedb::Time from, timedb::Time to):
	_chunks{},
	_from(from),
	_to(to),
	_flag(flag)
{
	_next.chunk = nullptr;
	_next.count = 0;
}

void timedb::storage::MemoryStorage::InnerReader::add(Chunk_Ptr c, size_t count)
{
	timedb::storage::MemoryStorage::InnerReader::ReadChunk rc;
	rc.chunk = c;
	rc.count = count;
	this->_chunks.push_back(rc);
}

bool timedb::storage::MemoryStorage::InnerReader::isEnd() const
{
	return this->_chunks.size() == 0 && _next.count==0;
}

bool  timedb::storage::MemoryStorage::InnerReader::check_meas(timedb::Meas&m) {
	if ((timedb::in_filter(_flag, m.flag))	&& (timedb::utils::inInterval(_from, _to, m.time))) {
		return true;
	}
	return false;
}

void timedb::storage::MemoryStorage::InnerReader::readNext(timedb::Meas::MeasList *output)
{
	if ((_next.chunk == nullptr) || (_next.count==0)) {
		if (_chunks.size() != 0) {
			auto fr = _chunks.front();
			_next.chunk = fr.chunk;
			_next.count = fr.count;
			_chunks.pop_front();

			if (check_meas(_next.chunk->first)) {
				output->push_back(_next.chunk->first);
			}
		}
	}

	while (_next.count != 0) {
		timedb::compression::CopmressedReader crr(
			timedb::compression::BinaryBuffer(_next.chunk->times.begin, _next.chunk->times.end),
			timedb::compression::BinaryBuffer(_next.chunk->values.begin, _next.chunk->values.end),
			timedb::compression::BinaryBuffer(_next.chunk->flags.begin, _next.chunk->flags.end), _next.chunk->first);

		for (size_t i = 0; i < _next.count; i++) {
			auto sub = crr.read();
			if (check_meas(sub)) {
				output->push_back(sub);
			}
		}
		_next.count = 0;
	}
}
