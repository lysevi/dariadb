#include "storage.h"
#include "utils.h"

memseries::storage::MemoryStorage::Block::Block(size_t size)
{
    begin = new uint8_t[size];
    end = begin + size;

    memset(begin, 0, size);
	bb = compression::BinaryBuffer(begin, end);
}

memseries::storage::MemoryStorage::Block::~Block()
{
    delete[] begin;
}

memseries::storage::MemoryStorage::MeasChunk::MeasChunk(size_t size, Meas first_m) :
	times(size),
	flags(size),
	values(size),
    count(0),
    first(first_m)
{
	time_compressor = compression::DeltaCompressor(times.bb);
	flag_compressor = compression::FlagCompressor(flags.bb);
	value_compressor = compression::XorCompressor(values.bb);
}

memseries::storage::MemoryStorage::MeasChunk::~MeasChunk()
{

}

bool memseries::storage::MemoryStorage::MeasChunk::append(const Meas & m)
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

memseries::storage::MemoryStorage::MemoryStorage(size_t size):
	_size(size),
	_min_time(std::numeric_limits<memseries::Time>::max()),
	_max_time(std::numeric_limits<memseries::Time>::min())
{
}


memseries::storage::MemoryStorage::~MemoryStorage()
{
	_chuncks.clear();
}

memseries::Time memseries::storage::MemoryStorage::minTime()
{
	return _min_time;
}

memseries::Time memseries::storage::MemoryStorage::maxTime()
{
	return _max_time;
}

memseries::append_result memseries::storage::MemoryStorage::append(const memseries::Meas &value)
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
	return memseries::append_result(1, 0);
}

memseries::append_result memseries::storage::MemoryStorage::append(const memseries::Meas::PMeas begin, const size_t size)
{
	memseries::append_result result{};
	for (size_t i = 0; i < size; i++) {
		result = result + append(begin[i]);
	}
	return result;
}

memseries::storage::Reader_ptr memseries::storage::MemoryStorage::readInterval(const memseries::IdArray &ids, memseries::Flag flag, memseries::Time from, memseries::Time to)
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

memseries::storage::Reader_ptr memseries::storage::MemoryStorage::readInTimePoint(const memseries::IdArray &ids, memseries::Flag flag, memseries::Time time_point)
{
	return this->readInterval(ids, flag, 0, time_point);
}


void memseries::storage::Reader::readAll(Meas::MeasList * output)
{
	while (!isEnd()) {
		readNext(output);
	}
}


memseries::append_result memseries::storage::AbstractStorage::append(const Meas::MeasArray & ma)
{
    memseries::append_result ar{};
    for(auto&m:ma){
        this->append(m);
    }
    return ar;
}

memseries::append_result memseries::storage::AbstractStorage::append(const Meas::MeasList & ml)
{
    memseries::append_result ar{};
    for(auto&m:ml){
        ar=ar+this->append(m);
    }
    return ar;
}

memseries::storage::Reader_ptr memseries::storage::AbstractStorage::readInterval(Time from, Time to)
{
    static memseries::IdArray empty_id{};
    return this->readInterval(empty_id,0,from,to);
}

memseries::storage::Reader_ptr memseries::storage::AbstractStorage::readInTimePoint(Time time_point)
{
    static memseries::IdArray empty_id{};
    return this->readInTimePoint(empty_id,0,time_point);
}



memseries::storage::MemoryStorage::InnerReader::InnerReader(memseries::Flag flag, memseries::Time from, memseries::Time to):
	_chunks{},
    _flag(flag),
    _from(from),
    _to(to)
{
	_next.chunk = nullptr;
	_next.count = 0;
}

void memseries::storage::MemoryStorage::InnerReader::add(Chunk_Ptr c, size_t count)
{
	memseries::storage::MemoryStorage::InnerReader::ReadChunk rc;
	rc.chunk = c;
	rc.count = count;
	this->_chunks.push_back(rc);
}

bool memseries::storage::MemoryStorage::InnerReader::isEnd() const
{
	return this->_chunks.size() == 0 && _next.count==0;
}

bool  memseries::storage::MemoryStorage::InnerReader::check_meas(memseries::Meas&m) {
	if ((memseries::in_filter(_flag, m.flag))	&& (memseries::utils::inInterval(_from, _to, m.time))) {
		return true;
	}
	return false;
}

void memseries::storage::MemoryStorage::InnerReader::readNext(memseries::Meas::MeasList *output)
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
		memseries::compression::CopmressedReader crr(
			memseries::compression::BinaryBuffer(_next.chunk->times.begin, _next.chunk->times.end),
			memseries::compression::BinaryBuffer(_next.chunk->values.begin, _next.chunk->values.end),
			memseries::compression::BinaryBuffer(_next.chunk->flags.begin, _next.chunk->flags.end), _next.chunk->first);

		for (size_t i = 0; i < _next.count; i++) {
			auto sub = crr.read();
			if (check_meas(sub)) {
				output->push_back(sub);
			}
		}
		_next.count = 0;
	}
}
